import { parse, transform, stringify } from 'csv'
import fs from "node:fs";
import z from 'zod';
import { logger } from './logging.js';
import dotenv from 'dotenv';

dotenv.config();

type ImportRecord = {
	email: string
	firstName: string
	lastName: string
	isSubscribed: string // "Yes" | "No"
	unsubscribedDate: string
}

const contactSchema = z.object({
	email: z.email(),
	firstName: z.string(),
	lastName: z.string(),
	isSubscribed: z.enum(['Yes', 'No']),
	unsubscribedDate: z.string().optional(),
})

const __dirname = new URL(".", import.meta.url).pathname; // like '.../src/' with trailing slash
const importDir = `${__dirname}../import/`
const exportDir = `${__dirname}../export/`

const createCsvParseTransform = () => parse<ImportRecord>({
	columns: ['email',
		'firstName',
		'lastName',
		'isSubscribed',
		'unsubscribedDate'
	],
})

const createFilterSubscribedTransform = () => transform((record) => {
	if (record.email === 'Email Address') {
		// Skip header row
		return null
	}

	const parseRes = contactSchema.safeParse(record)
	if (!parseRes.success) {
		logger.error(z.treeifyError(parseRes.error), `Invalid record: ${JSON.stringify(record)}`)
		return null
	}

	const validRecord = parseRes.data
	if (validRecord.isSubscribed === 'Yes') {
		return validRecord
		// return [validRecord.email,
		// validRecord.firstName,
		// validRecord.lastName,
		// validRecord.isSubscribed,
		// validRecord.unsubscribedDate || '']
	} else {
		logger.debug(validRecord, `Unsubscribed contact, skipping`)
		return null
	}
})

const createCsvStringifyTransform = () => stringify({
	header: true
})

const processFile = (inputFilePath: string, outputFilePath: string) => {
	const inputStream = fs.createReadStream(inputFilePath)
	const outputStream = fs.createWriteStream(outputFilePath)
	const pipeline = inputStream
		.pipe(createCsvParseTransform())
		.pipe(createFilterSubscribedTransform())
		.pipe(createCsvStringifyTransform())
		.pipe(outputStream)

	return new Promise<void>((resolve, reject) => {
		pipeline.on('finish', () => {
			logger.info(`Finished processing file: ${inputFilePath}`)
			resolve()
		})
		pipeline.on('error', (err) => {
			logger.error(err, `Error processing file: ${inputFilePath}`)
			reject(err)
		})
	})
}

/**
 * Main function
 * Parses all CSV files in the import directory, filters subscribed contacts,
 * and writes the results to the export directory.
 */
const main = async () => {
	const allFiles = fs.readdirSync(importDir)
	const csvFiles = allFiles.filter((file) => file.endsWith('.csv'))
	logger.info(`Number of files to process: ${csvFiles.length}`)

	for (const file of csvFiles) {
		logger.info(`Processing file: ${file}`)
		const inputFilePath = `${importDir}${file}`
		const outputFilePath = `${exportDir}${file}`
		await processFile(inputFilePath, outputFilePath)
		await new Promise((res) => setTimeout(res, 1000)) // 1 second delay between files
	}
}

main()
	.then(() => {
		console.log('Filtering subscribers done')
	})
	.catch((err) => {
		console.error('Filtering subscribers error: ', err)
	})