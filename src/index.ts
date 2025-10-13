import { generate } from 'csv'

const main = () => {
    generate({
        columns: ["int", "bool"],
        length: 2,
    }).pipe(process.stdout);
}

main()