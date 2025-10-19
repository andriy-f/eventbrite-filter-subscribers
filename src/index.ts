import { parse } from 'csv'
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

const contactSchema = z.object({
    email: z.email(),
    firstName: z.string(),
    lastName: z.string(),
    isSubscribed: z.enum(['Yes', 'No']),
    unsubscribedDate: z.string().optional(),
})

const __dirname = new URL(".", import.meta.url).pathname; // like '.../src/' with trailing slash
const importDir = `${__dirname}../import/`

const getFileReader = async (fileName: string) => {
    const readStream = fs.createReadStream(`${importDir}${fileName}`)
    return readStream
}

const main = async () => {
    const allFiles = fs.readdirSync(importDir)
    const csvFiles = allFiles.filter((file) => file.endsWith('.csv'))
    console.log('Number of files to process:', csvFiles.length)


    for (const file of csvFiles) {
        logger.debug(`Processing file: ${file}`)
        const fileStream = await getFileReader(file)
        const parser = fileStream.pipe(parse<ImportRecord>({
            // columns: true,
            columns: ['email',
                'firstName',
                'lastName',
                'isSubscribed',
                'unsubscribedDate'
            ],
        }))

        for await (const record of parser) {
            if (record.email === 'Email Address') {
                // skip header row
                continue
            }

            const result = contactSchema.safeParse(record)
            if (!result.success) {
                logger.error(z.treeifyError(result.error), `Invalid record in file ${file}: ${JSON.stringify(record)}`)
                continue
            }
            const validRecord = result.data
            // logger.info(validRecord, `Valid record found in file ${file}`)
        }
    }
}

main()
    .then(() => {
        console.log('filtering subscribers done')
    })
    .catch((err) => {
        console.error('Filtering subscribers error: ', err)
    })