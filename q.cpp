#include <iostream>
#include <gtest/gtest.h>
#include <fstream>
#include <vector>
#include <string>
#include <sstream>
#include <pthread.h>  // For threads
#include <semaphore.h> // For semaphores

using namespace std;

// Structure to store key-value pairs
struct KeyValue 
{
    string key;
    int value;

    KeyValue(string k, int v) : key(k), value(v) {}  // Constructor to initialize key and value
};

// Shared semaphore and mutex for synchronization
sem_t semaphoreLock;  // Semaphore to ensure thread-safe operations
pthread_mutex_t outputMutex = PTHREAD_MUTEX_INITIALIZER;  // Mutex to lock output section

// Predefined spaces for alignment in the output
const string ALIGN_SPACES = "                             ";

// Function to split input text into smaller chunks based on a defined chunk size
vector<string> splitTextIntoChunks(const string &inputText, size_t chunkSize) 
{
    vector<string> chunks;  // Vector to store chunks of text
    stringstream textStream(inputText);  // Stream to read input text word by word
    string word, currentChunk;

    size_t currentSize = 0;  // Track current chunk size
    while (textStream >> word) 
    {
        if (currentSize + word.size() + 1 > chunkSize) 
        {  // If adding the word exceeds chunk size
            chunks.push_back(currentChunk);  // Add the chunk to the list
            currentChunk.clear();  // Reset current chunk
            currentSize = 0;
        }
        currentChunk += word + " ";  // Add word and space to the current chunk
        currentSize += word.size() + 1;
    }

    if (!currentChunk.empty()) 
    {  // Add the last chunk if it's not empty
        chunks.push_back(currentChunk);
    }

    return chunks;  // Return all created chunks
}

// Mapping Phase: Process a chunk of text and create key-value pairs
void *generateKeyValuePairs(void *arg) 
{
    // Extract arguments
    pair<string, vector<KeyValue> *> *params = (pair<string, vector<KeyValue> *> *)arg;  // Cast arguments
    string chunk = params->first;  // Extract chunk text
    vector<KeyValue> *keyValueStore = params->second;  // Extract shared key-value storage

    stringstream chunkStream(chunk);
    string word;

    // Iterate through the chunk and generate key-value pairs
    while (chunkStream >> word) 
    {
        sem_wait(&semaphoreLock);  // Lock access to shared storage
        keyValueStore->push_back(KeyValue(word, 1));  // Add (word, 1) pair
        sem_post(&semaphoreLock);  // Unlock access to shared storage
    }

    delete params;  // Free allocated memory
    pthread_exit(NULL);  // Exit the thread
}

// Shuffling Phase: Group key-value pairs by key and aggregate their values
void groupAndAggregateKeyValuePairs(const vector<KeyValue> &keyValues, vector<KeyValue> &groupedResults) 
{
    for (const auto &pair : keyValues) 
    {
        bool found = false;

        // Search for an existing key in grouped results
        for (auto &result : groupedResults) 
        {
            if (result.key == pair.key) 
            {
                result.value += pair.value;  // Aggregate the value
                found = true;
                break;
            }
        }

        if (!found) 
        {
            groupedResults.push_back(pair);  // Add a new key-value pair
        }
    }
}

// Reducing Phase: Display reduced results
void *displayReducedResults(void *arg) 
{
    vector<KeyValue> *groupedResults = (vector<KeyValue> *)arg;  // Cast the grouped results

    pthread_mutex_lock(&outputMutex);  // Lock the output section
    cout << "\n" << ALIGN_SPACES << "=======================================================\n";
    cout << ALIGN_SPACES << "                FINAL REDUCED RESULTS               \n";
    cout << ALIGN_SPACES << "=======================================================\n";
   
    for (const auto &result : *groupedResults) 
    {
        cout << ALIGN_SPACES << "(" << result.key << ", " << result.value << ")" << endl;
    }
    pthread_mutex_unlock(&outputMutex);  // Unlock the output section

    pthread_exit(NULL);  // Exit the thread
}

// Mapping Controller: Distribute text chunks across threads for processing
void processChunksUsingThreads(const vector<string> &chunks, vector<KeyValue> &keyValueStore) 
{
    vector<pthread_t> threads(chunks.size());  // Create threads for each chunk

    for (size_t i = 0; i < chunks.size(); i++) 
    {
        pair<string, vector<KeyValue> *> *params = new pair<string, vector<KeyValue> *>(chunks[i], &keyValueStore);
        pthread_create(&threads[i], NULL, generateKeyValuePairs, (void *)params);  // Start a thread for each chunk
    }

    for (pthread_t &thread : threads) 
    {
        pthread_join(thread, NULL);  // Wait for all threads to complete
    }
}

TEST(TextProcessingTest, SplitTextTest) 
{
    string input = "The quick brown fox jumps";
    vector<string> chunks = splitTextIntoChunks(input, 5);
    EXPECT_EQ(chunks.size(), 5);
}

TEST(ShufflePhaseTest, GroupTextTest)
{
    vector<KeyValue> keyValues = {{"The", 1}, {"The", 1}, {"quick", 1}};
    vector<KeyValue> groupedResults;
    groupAndAggregateKeyValuePairs(keyValues, groupedResults);
    EXPECT_EQ(groupedResults.size(), 2);
    EXPECT_EQ(groupedResults[0].key, "The");
    EXPECT_EQ(groupedResults[0].value, 2);
}

int main(int argc, char **argv) 
{
    sem_init(&semaphoreLock, 0, 1);  // Initialize semaphore
    ::testing::InitGoogleTest(&argc, argv);
    int result = RUN_ALL_TESTS();
    sem_destroy(&semaphoreLock);  // Destroy semaphore
    return result;
}





/*int main() {
    vector<string> files = {"fil1.txt", "fil2.txt", "fil3.txt", "fil4.txt", "fil5.txt"};  // List of files to process
    size_t chunkSize = 11;  // Define chunk size for splitting
    vector<KeyValue> globalKeyValueStore;  // Global store for aggregated results

    // Initialize semaphore
    sem_init(&semaphoreLock, 0, 1);  // Binary semaphore for thread-safe operations

    cout << ALIGN_SPACES << "=======================================================\n";
    cout << ALIGN_SPACES << "                MAPREDUCE PROCESS STARTED              \n";
    cout << ALIGN_SPACES << "=======================================================\n";

    for (size_t fileIndex = 0; fileIndex < files.size(); fileIndex++) 
    {
        ifstream file(files[fileIndex]);
        if (!file.is_open()) 
        {
            cerr << ALIGN_SPACES << "Error: Unable to open file " << files[fileIndex] << endl;
            continue;
        }

        string fileContent((istreambuf_iterator<char>(file)), istreambuf_iterator<char>());
        file.close();

        if (fileContent.empty())
         {
            cerr << ALIGN_SPACES << "Warning: File " << files[fileIndex] << " is empty." << endl;
            continue;
        }

        cout << "\n" << ALIGN_SPACES << "-------------------------------------------------------\n";
        cout << ALIGN_SPACES << "Processing File: " << files[fileIndex] << endl;

        // Splitting Phase
        vector<string> chunks = splitTextIntoChunks(fileContent, chunkSize);
        cout << ALIGN_SPACES << "Chunks Created:" << endl;
        for (size_t i = 0; i < chunks.size(); i++) 
        {
            cout << ALIGN_SPACES << "  Chunk " << i + 1 << ": \"" << chunks[i] << "\"" << endl;
        }

        // Mapping Phase
        vector<KeyValue> keyValues;
        processChunksUsingThreads(chunks, keyValues);

        // Shuffling Phase
        vector<KeyValue> groupedResults;
        groupAndAggregateKeyValuePairs(keyValues, groupedResults);

        cout << ALIGN_SPACES << "Shuffling Results:" << endl;
        for (size_t i = 0; i < groupedResults.size(); i++) 
        {
            cout << ALIGN_SPACES << "  (" << groupedResults[i].key << ", " << groupedResults[i].value << ")" << endl;
        }

        // Add grouped results to the global store
        globalKeyValueStore.insert(globalKeyValueStore.end(), groupedResults.begin(), groupedResults.end());
    }

    // Global Shuffling Phase
    vector<KeyValue> finalGroupedResults;
    groupAndAggregateKeyValuePairs(globalKeyValueStore, finalGroupedResults);

    cout << "\n" << ALIGN_SPACES << "=======================================================\n";
    cout << ALIGN_SPACES << "                GLOBAL SHUFFLING RESULTS               \n";
    cout << ALIGN_SPACES << "=======================================================\n";
    for (size_t i = 0; i < finalGroupedResults.size(); i++) 
    {
        cout << ALIGN_SPACES << "  (" << finalGroupedResults[i].key << ", " << finalGroupedResults[i].value << ")" << endl;
    }

    // Reducing Phase
    pthread_t reducerThread;
    pthread_create(&reducerThread, NULL, displayReducedResults, (void *)&finalGroupedResults);
    pthread_join(reducerThread, NULL);

    // Destroy semaphore
    sem_destroy(&semaphoreLock);

    cout << ALIGN_SPACES << "=====================================================================\n";
    cout << ALIGN_SPACES << "                 MAPREDUCE PROCESS COMPLETE            \n";
    cout << ALIGN_SPACES << "=====================================================================\n";

    return 0;
}*/
