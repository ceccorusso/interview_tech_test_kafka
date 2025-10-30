const {Kafka} = require('kafkajs');
const fs = require('fs');
const path = require('path');

// Kafka configuration
const kafka = new Kafka({
    clientId: 'csv-consumer', brokers: ['localhost:9092'], // Default Kafka broker address
});

// Create a Kafka consumer
const consumer = kafka.consumer({groupId: 'csv-reconstruction-group'});

// Dictionary to store a writable stream for each CSV file
const writableStream = new Map();

// Default Kafka topic to consume from
const topic = 'raw-transactions';

// Variable to track total messages received
let totalMessagesReceived = 0;

// Variable to track total files reconstructed
let totalFilesReconstructed = 0;

// Function to convert JSON object to CSV row
function jsonToCSVRow(jsonObj) {

    const values = Object.values(jsonObj).map(value => {

        // Handle values that might contain commas or quotes
        if (typeof value === 'string' && (value.includes(',') || value.includes('"') || value.includes('\n'))) {
            return `"${value.replace(/"/g, '""')}"`;
        }

        return value;
    });

    return values.join(',');
}

// Function to process each Kafka message consumed
function processMessage(message) {
    try {
        const value = JSON.parse(message.value.toString());

        // Extract the source file name from the message headers
        const source = message.headers['source'].toString();

        // Check if a writable stream for the source file exists
        if (!writableStream.has(source)) {

            const stream = fs.createWriteStream(path.join(__dirname, source))

            const header = Object.keys(value)

            // Write the header row
            stream.write(header.join(',') + '\n')

            // Store the writable stream for the source
            writableStream.set(source, stream);

            totalFilesReconstructed++;
        }

        const stream = writableStream.get(source)

        const csvRow = jsonToCSVRow(value);

        // Write the CSV row to the file
        stream.write(csvRow + '\n');

        totalMessagesReceived++;
    } catch (error) {
        console.error(`❌ Error processing message ${message}:`, error);
    }
}

// Simple function to clean up existing CSV files
function cleanupExistingCSVFiles() {
    fs.readdirSync(__dirname)
        .filter(file => file.endsWith('.csv'))
        .map(file => fs.unlinkSync(path.join(__dirname, file)));
}

// Main consumer function
async function main() {
    try {

        cleanupExistingCSVFiles();

        console.log('🚀 Starting CSV reconstruction consumer...');

        // Connect to Kafka
        await consumer.connect();
        console.log('✅ Connected to Kafka');

        // Subscribe to the topic
        await consumer.subscribe({
            topic: topic, fromBeginning: true // Start from the beginning to catch all messages
        });

        console.log(`📡 Subscribed to ${topic} topic`);

        // Start consuming messages
        await consumer.run({
            eachMessage: async ({topic, partition, message}) => {

                console.log(`New message: ${message.value.toString()}...`);

                processMessage(message);

                if (totalMessagesReceived % 10 === 0) {
                    console.log(`📈 Progress: ${totalMessagesReceived} messages received`);
                }
            },
        });

    } catch (error) {
        console.error('❌ Error in consumer:', error);
    }
}

// Function to handle graceful shutdown
async function gracefulShutdown() {
    console.log('\n🛑 Shutting down gracefully...');

    if (writableStream.size > 0) {

        // Close all writable streams
        for (const [filename, stream] of writableStream.entries()) {
            console.log(`Closing writable stream for ${filename}...`);
            stream.close();
        }
    }

    // Disconnect from Kafka
    await consumer.disconnect();
    console.log('✅ Disconnected from Kafka');
    console.log('👋 Consumer shutdown complete');

    console.log(`\n📊 Summary:`);
    console.log(`   Total messages received: ${totalMessagesReceived}`);
    console.log(`   Files reconstructed: ${totalFilesReconstructed}`);
}

// Handle graceful shutdown signals
process.on('SIGINT', async () => {
    console.log('\n📡 Received SIGINT, shutting down gracefully...');
    await gracefulShutdown();
    process.exit(0);
});

process.on('SIGTERM', async () => {
    console.log('\n📡 Received SIGTERM, shutting down gracefully...');
    await gracefulShutdown();
    process.exit(0);
});

// Handle uncaught exceptions
process.on('uncaughtException', async (error) => {
    console.error('💥 Uncaught Exception:', error);
    await gracefulShutdown();
    process.exit(1);
});

process.on('unhandledRejection', async (reason, promise) => {
    console.error('💥 Unhandled Rejection at:', promise, 'reason:', reason);
    await gracefulShutdown();
    process.exit(1);
});

// Run the main function
if (require.main === module) {
    main().catch(async (error) => {
        console.error('💥 Fatal error:', error);
        await gracefulShutdown();
        process.exit(1);
    });
}
