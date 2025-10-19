import { parse } from 'csv'
import fs from "node:fs";

type ImportRecord = {
    email: string
    firstName: string
    lastName: string
    isSubscribed: string // "Yes" | "No"
    unsubscribedDate: string
}

const __dirname = new URL(".", import.meta.url).pathname; // like '.../src/' with trailing slash

const getFileReader = async (fileName: string) => {
    const readStream = fs.createReadStream(`${__dirname}../import/${fileName}`)
    return readStream
}

const main = async () => {
    const allFiles = fs.readdirSync(`${__dirname}../import`)
    const csvFiles = allFiles.filter((file) => file.endsWith('.csv'))
    console.log('Number of files to process:', csvFiles.length)


    for (const file of csvFiles) {
        console.log('Processing file:', file)
        const fileStream = await getFileReader(file)
        const parser = fileStream.pipe(parse({
            // columns: true,
            columns: ['email',
                'firstName',
                'lastName',
                'isSubscribed',
                'unsubscribedDate'
            ],
        }))

        for await (const record of parser) {
            // console.log(record)
        }
    }
}

main()
    .then(() => {
        console.log('Done')
    })
    .catch((err) => {
        console.error('Error:', err)
    })