#include <iostream>
#include <fstream>
#include <vector>
#include <queue>
#include <string>
#include <regex>
#include <dirent.h>

int OPTIMAL_FILE_OPENINGS = 3; //According to the question windows Operating systems in case of optimal working of
                               //the subsystem we cannot open more that 100 files so the variable takes indicates number of 
                               //files opened by the code at one time;
int chunkSize = 7;           //Since we cannot bring all of the data into the RAM/main memory we need to bring the data
                             //In this case the number of orders to be brought in the main memory at one point of time

// Function to extract the symbol of singular order
void getSymbol(std::string& str,std::string& orderSymbol){ 
   for(int i =0;i<str.size();i++){
      if(str[i]==','){
          orderSymbol=str.substr(0,i-1);
          return ;
      }
   }
}
 //Function to extract the timestamp of singular order
void getTimeStamp(std::string& str,std::string& orderTimeStamp){  
    int first_delimiter = str.find(",");
    orderTimeStamp = str.substr(first_delimiter+2);
    int second_delimiter = orderTimeStamp.find(",");
    orderTimeStamp=orderTimeStamp.substr(0,second_delimiter);
    return;
}

//Custom comparator while sorting the orders from different files in chuks
struct comp{
    //Overloading the "()" operator so that it can be used as a functor 
    //Checking for timestamp first then if they are equal go for the symbol 
    //Ordering in Ascending order for both timestamp and symbol
    bool operator()(std::pair<std::string,int> order1,std::pair<std::string,int> order2){
        std::string timeStampOrder1,timeStampOrder2,symbolOrder1,symbolOrder2;
        getTimeStamp(order1.first,timeStampOrder1);
        getTimeStamp(order2.first,timeStampOrder2);
        getSymbol(order1.first,symbolOrder1);
        getSymbol(order2.first,symbolOrder2);
        if(timeStampOrder1>timeStampOrder2){
            return true;
        }
        else if(timeStampOrder1==timeStampOrder2){
            if(symbolOrder1>symbolOrder2){
                return true;
            }
            else{
                return false;
            }
        }
        else{
            return false;
        }
    }
};
//Function to create chunks of the current batch of files to load the chunk and populate the output file
void createChunk(const std::string& inputFile,const std::string& outputFile,int chunkSize){
    std::ifstream input(inputFile);
    std::ofstream output(outputFile);
    std::vector<std::string> orders;
    std::string value;
    //Creating chunk of size ChunkSize or less than that (UPPER CAP chunksize)
    while(getline(input,value)){
        orders.push_back(value);
        if(orders.size() == chunkSize){
            for(const auto& val:orders){
                output<<val<<"\n";
            }
            orders.clear();
        }
    }
    if(!orders.empty()){
        for(const auto& val:orders){
            output<<val<<"\n";
        }
    }
}
//Merge the sorted file batch :The size of the batch is always less than equal to the number of OPTIMAL_FILE_OPENINGS
void mergeSortedFiles(const std::vector<std::string>& inputFiles,const std::string& outputFile){
    std::vector<std::string> chunkFiles;
    for(const auto& inputFile:inputFiles){
        std::string chunkFile = "chunk_"+inputFile;
        createChunk(inputFile,chunkFile,chunkSize);
        chunkFiles.push_back(chunkFile); 
    }

    std::vector<std::ifstream> inputs;
    for(const auto& chunkFile:chunkFiles){
        inputs.emplace_back(chunkFile);
    }
    std::ofstream output(outputFile);
    //Use of Priority Queue with custom comparator while sorting the already sorted chunks -> making pair for the 
    //File stream and the actual chunk to -> making pair to store the actual stream order is coming from
    std::priority_queue<std::pair<std::string,int>,std::vector<std::pair<std::string,int>>,comp> minHeap;
    //Pushing chunks into the priority Queue
    for(int i =0; i<inputs.size();i++){
        std::string value;
        if(getline(inputs[i],value)){
            minHeap.push(std::make_pair(value,i));
        }
    }
    //Sorting the current files
    while(!minHeap.empty()){
        std::pair<std::string,int> current = minHeap.top();
        minHeap.pop();
        output << current.first<<"\n";

        std::string value;
        if(getline(inputs[current.second],value)){
            minHeap.push(std::make_pair(value, current.second));
        }
    }
    //Closing input streams created 
    for(auto &input :inputs){
        input.close();
    }
    //Closing the current output for the batch file
    output.close();
    //Removing intermediate chunk files which are not needed after the current batches are sorted
    for(const auto& chunkFile:chunkFiles){
        remove(chunkFile.c_str());
    }
}
//File Batching to make sure that there are only OPTIMAL_FILE_OPENINGS and the rest of the files are waiting in the queue
void fileBatching(const std::vector<std::string>& files,const std::string& outputFile){
    std::queue<std::string> file_batch;
    for(int i=0;i<files.size();i++){
        file_batch.push(files[i]);
    }
    //String vectors to store the current batch and the file names that are generated by intermediate matchung
    std::vector<std::string> current_batch;
    std::vector<std::string> batch_file_names;
    //To store the batch files according to the run index 
    int run = 0;  
    //Batching using STL queue to open optimal number of files
    while(!file_batch.empty()){
        std::string temp = file_batch.front();
        current_batch.push_back(temp);
        file_batch.pop();
        if(current_batch.size()==OPTIMAL_FILE_OPENINGS){
            run++;
            std::string batch_string = "BATCH_"+std::to_string(run);
            mergeSortedFiles(current_batch,batch_string);
            batch_file_names.push_back(batch_string);
            current_batch.clear();
            file_batch.push(batch_string);
        }
    }
    //Batching of the last set which must be less than OPTIMAL_FILE_OPENINGS according to the variant
    if(current_batch.size()!=0){
        run++;
        std::string batch_string = "BATCH_"+std::to_string(run);
        mergeSortedFiles(current_batch,batch_string);
        file_batch.push(batch_string);
        batch_file_names.push_back(batch_string);        
    }
    //Clearing the batch after the last batch has been merged
    current_batch.clear();

    std::string compressedFile = file_batch.front();
    std::ofstream output_file_stream(outputFile);
    std::ifstream input_file_stream(compressedFile);
    std::string ss;
    output_file_stream<<"Symbol, Timestamp, Price, Size, Exchange, Type"<<"\n";
    //Streaming final output into the multiplexed File
    while(getline(input_file_stream,ss)){
        output_file_stream<<ss<<"\n";
    }
    //Closing input and output streams for the current file
    output_file_stream.close();
    input_file_stream.close();
    //Removng the batch files that where created intermediately
    for(const auto& batchFiles:batch_file_names){
        remove(batchFiles.c_str());
    }
}
//Main Entry Point of the function searching for txt files/singular order files via regex matching taking in the path and
//name and output file inside the  function 
int main(){
    std::string outputFile = "multiplexedFile.txt"; // Name of output file
    std::regex rx(".*\\.txt$");
    const  char* path = "C:/Users/admin/Desktop/programs/MultiplexFiles"; // Path to where singular files/Order Files are placed
    std::vector<std::string> files;
    struct dirent *entry;
    DIR *dir = opendir(path);
    if(dir == NULL){
        std::cout<<"FILES NOT FOUND"<<"\n";
        return 0;
    }
    //Storing all the singular/Order files in the path given above
    while((entry = readdir(dir))!=NULL){
        if(std::regex_match(entry->d_name,rx)){
            if(entry->d_name!=outputFile){
                files.push_back(entry->d_name);
            }           
        }
    }
    closedir(dir);
    //Creating the symbolised text file--> tagging the symbol to every order
    std::vector<std::string> file_paths ;
    for(int i=0;i<files.size();i++){
        std::ifstream file_stream(files[i]);
        std::ofstream out_stream("S_"+files[i]);
        file_paths.push_back("S_"+files[i]);
        std::string ss;
        std::string sym="";
        int index = files[i].find(".");
        if(index!=std::string::npos){
            sym = files[i].substr(0,index);
        }
        getline(file_stream,ss);
        while(getline(file_stream,ss)){
            out_stream<<sym<<", "<<ss<<"\n";
        }
        file_stream.close();
        out_stream.close();
    }
    //Calling the fileBatching function to Batch files for sorting 
    fileBatching(file_paths,outputFile);
    //Removing the symbolised files 
    for(const auto& symbolisedFile:file_paths){
        remove(symbolisedFile.c_str());
    }
    return 0;
}
