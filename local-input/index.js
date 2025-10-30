const {Kafka} = require('kafkajs');
const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');

// Kafka configuration
const kafka = new Kafka({
    clientId: 'csv-producer', brokers: ['localhost:9092'], // Default Kafka broker address
});

// Default Kafka topic
const topic = 'raw-transactions';

// Create a Kafka producer
const producer = kafka.producer();

// Process each CSV file sending data to Kafka
async function processCSVFile(filePath) {
    return new Promise((resolve, reject) => {

        const source = path.basename(filePath)

        let numberOfRows = 0;

        fs.createReadStream(filePath)
            .pipe(csv())
            .on('data', async (data) => {

                numberOfRows++;

                const messages = [{
                    key: source.replace('.csv', ''),  // Partition key is the source file name without the extension
                    value: JSON.stringify(data), headers: {
                        source: source, timestamp: new Date().toISOString()
                    }
                }]

                await producer.send({
                    topic: topic, messages: messages
                });
            })
            .on('end', () => {
                resolve(numberOfRows)
            })
            .on('error', (error) => {
                console.error(`Error reading ${filePath}:`, error);
                reject(error);
            });
    });
}

// Main function
async function main() {
    try {
        console.log('Starting CSV to Kafka producer...');

        // Connect to Kafka
        await producer.connect();
        console.log('Connected to Kafka');

        // Find all CSV files in the current directory
        const csvFiles = fs.readdirSync(__dirname)
            .filter(file => file.endsWith('.csv'))
            .map(file => path.join(__dirname, file));

        if (csvFiles.length === 0) {
            console.log('No CSV files found in the current directory');
            return;
        }

        console.log(`Found ${csvFiles.length} CSV files:`, csvFiles.map(f => path.basename(f)));

        // Process each CSV file
        for (const csvFile of csvFiles) {

            const basename = path.basename(csvFile);

            console.log(`\nProcessing ${basename}...`);

            try {
                const numberOfRows = await processCSVFile(csvFile);

                console.log(`Stored ${numberOfRows} rows for ${basename}`);
            } catch (error) {
                console.error(`Failed to process ${csvFile}:`, error);
                // Continue with other files even if one fails
            }
        }

        console.log('\nAll CSV files processed successfully!');

    } catch (error) {
        console.error('Error in main process:', error);
    } finally {
        // Disconnect from Kafka
        await producer.disconnect();
        console.log('Disconnected from Kafka');
    }
}

// Handle graceful shutdown
process.on('SIGINT', async () => {
    console.log('\nReceived SIGINT, shutting down gracefully...');
    await producer.disconnect();
    process.exit(0);
});

process.on('SIGTERM', async () => {
    console.log('\nReceived SIGTERM, shutting down gracefully...');
    await producer.disconnect();
    process.exit(0);
});

// Run the main function
if (require.main === module) {
    main().catch(console.error);
}