import { parse, transform, stringify } from 'csv'
import fs from "node:fs";
import z from 'zod';
import { logger } from './logging.js';

type ImportRecord = {
	email: string
	firstName: string
	lastName: string
	isSubscribed: string // "Yes" | "No"
	unsubscribedDate: string
}

type Location = {
	countryCode: string
	stateCode: string
	city: string
	countriesOfInterest: string
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

const fileNameToLocation = (fileName: string): Location => {
	const fileNameLowercase = fileName.toLowerCase()

	if (fileNameLowercase.includes('berlin')) {
		return { countryCode: 'DE', stateCode: '', city: 'Berlin', countriesOfInterest: 'Germany' }
	} else if (fileNameLowercase.includes('dresden')) {
		return { countryCode: 'DE', stateCode: 'Saxony', city: 'Dresden', countriesOfInterest: 'Germany' }
	} else if (fileNameLowercase.includes('munich') || fileNameLowercase.includes('munchen')) {
		return { countryCode: 'DE', stateCode: '', city: 'Munich', countriesOfInterest: 'Germany' }
	} else if (fileNameLowercase.includes('nurenberg') || fileNameLowercase.includes('nuernberg') || fileNameLowercase.includes('nuremberg')) {
		return { countryCode: 'DE', stateCode: '', city: 'Nurenberg', countriesOfInterest: 'Germany' }
	} else if (fileNameLowercase.includes('london')) {
		return { countryCode: 'GB', stateCode: '', city: 'London', countriesOfInterest: 'United Kingdom' }
	} else {
		return { countryCode: '', stateCode: '', city: '', countriesOfInterest: '' }
	}
}

const createCsvParseTransform = () => parse<ImportRecord>({
	columns: ['email',
		'firstName',
		'lastName',
		'isSubscribed',
		'unsubscribedDate'
	],
})

const createFilterSubscribedTransform = (location?: Location) => transform((record) => {
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
		return {
			...validRecord,
			...location
		}
	} else {
		logger.debug(validRecord, `Unsubscribed contact, skipping`)
		return null
	}
})

const createCsvStringifyTransform = () => stringify({
	header: true
})

const processFile = (inputFilePath: string, outputFilePath: string, location?: Location) => {
	const inputStream = fs.createReadStream(inputFilePath)
	const outputStream = fs.createWriteStream(outputFilePath)
	const pipeline = inputStream
		.pipe(createCsvParseTransform())
		.pipe(createFilterSubscribedTransform(location))
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
		const location = fileNameToLocation(file)
		await processFile(inputFilePath, outputFilePath, location)
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