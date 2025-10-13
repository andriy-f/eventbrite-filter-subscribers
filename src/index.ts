import { generate, parse } from 'csv'
import fs from "node:fs";

const __dirname = new URL(".", import.meta.url).pathname;

const getFileReader = async (fileName: string) => {
    const readStream = fs.createReadStream(`${__dirname}/../import/${fileName}`)
    return readStream
}

const main = async () => {
    const allFiles = fs.readdirSync(`${__dirname}/../import`)
    const csvFiles = allFiles.filter((file) => file.endsWith('.csv'))
    console.log('Number of files to process:', csvFiles.length)


    for (const file of csvFiles) {
        console.log('Processing file:', file)
        const fileStream = await getFileReader(file)
        const parser = fileStream.pipe(parse())

        // for await (const record of parser) {
        //     console.log(record)
        // }
    }
}

main()
    .then(() => {
        console.log('Done')
    })
    .catch((err) => {
        console.error('Error:', err)
    })