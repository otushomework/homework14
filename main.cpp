#include <iostream>
#include <stdlib.h>
#include <fstream>
#include <vector>
#include <map>
#include <future>
#include <algorithm>

#define _DEBUG

using FunctorContainer = std::vector<std::string>;
using ResultsContainer = std::vector<FunctorContainer>;

class MapFunctor
{
public:
    FunctorContainer operator()(std::string &line)
    {
        FunctorContainer list;

        std::transform(line.begin(), line.end(), line.begin(), ::tolower);
        for(char& c : line)
        {
            list.push_back(( !list.empty() ? list.at(list.size() - 1 )
                                          : std::string())
                           + std::string(&c, 1));

#ifdef _DEBUG
            std::cout << list.at(list.size() - 1) << std::endl;
#endif
        }

        //for empty strings return empty list
        return list;
    }
};

class ReduceFunctor
{
public:
    ReduceFunctor()
        : m_length(0)
    {

    }

    FunctorContainer operator()(std::string &line)
    {
        //list of one element, will be inserted to another FunctorContainer
        FunctorContainer list;

        if ( m_prevLine == line )
        {
            if (line.length() > m_length)
            {
                m_length = line.length()+1;
#ifdef _DEBUG
                list.push_back(std::to_string(m_length) + " " + line);
#else
                list.push_back(std::to_string(m_length));
#endif
            }
        }

        m_prevLine = line;

        return list;
    }

private:
    int m_length;
    std::string m_prevLine;
};

template <class TMapper, class TReducer>
class TextFileMapReduceFramework
{
public:
    TextFileMapReduceFramework(std::string filePath, int mnum, int rnum)
        : m_filePath(filePath)
        , m_mnum(mnum)
        , m_rnum(rnum)
    {

    }

    int run()
    {
        //check file and get file size
        std::ifstream file(m_filePath, std::ios::ate | std::ios::binary);
        if(!file.is_open())
        {
            std::cout << "No such file: " << m_filePath << std::endl;
            return 1;
        }
        std::streampos fileSize = file.tellg();
        int blockSize = fileSize/m_mnum;
        file.close();

#ifdef _DEBUG
        std::cout << "File: " << m_filePath << ". File size: " << fileSize << ". Block size: " << blockSize << std::endl;
#endif

        auto mapResults = map(blockSize);

#ifdef _DEBUG
        std::cout << "Map results count (" << mapResults.size() << "): ";
        int total = 0;
        for (auto &result : mapResults)
        {
            std::cout << result.size() << " ";
            total += result.size();
        }
        std::cout << "= " << total << std::endl;
#endif

        auto shuffleResults = shuffle(mapResults);

#ifdef _DEBUG
        std::cout << "Shuffle results count (" << shuffleResults.size() << "): ";
        total = 0;
        for (auto &result : shuffleResults)
        {
            std::cout << result.size() << " ";
            total += result.size();
        }
        std::cout << "= " << total << std::endl;
#endif

        auto reduceResults = reduce(shuffleResults);

#ifdef _DEBUG
        std::cout << "Reduce results count (" << reduceResults.size() << "): ";
        total = 0;
        for (auto &result : reduceResults)
        {
            std::cout << result.size() << " ";
            total += result.size();
        }
        std::cout << "= " << total << std::endl;
#endif
        //write reduce output to files
        for (int i = 0; i < reduceResults.size(); ++i)
        {
            std::ofstream file("reduce_" + std::to_string(i) + ".txt");

            for(const auto& line : reduceResults[i])
            {
                file << line << std::endl;
#ifdef _DEBUG
                std::cout << line << std::endl;
#endif
            }
            file.close();
        }

        return 0;
    }

    auto map(int blockSize)
    {
        ResultsContainer mapResults;
        std::vector<std::future<FunctorContainer> > futures(m_mnum);

        for (size_t i = 0; i < m_mnum; ++i)
        {
            futures[i] = std::async([this](int blockSize, int offset)
            {
                std::ifstream file(m_filePath);
                if (!file.is_open())
                {
                    std::cout << "No such file: " << m_filePath << std::endl;
                    return FunctorContainer();
                }

                int offsetStart = offset == 0 ? 0 : offset*blockSize;
                int offsetEnd = offset == 0 ? blockSize : offset*blockSize + blockSize;

                //move offsetStart and offsetStart to end of line
                moveOffsetToEndOfLine(file, offsetStart);
                moveOffsetToEndOfLine(file, offsetEnd);

                file.seekg(offsetStart, std::ios_base::beg);

                //exec user mapper functor
                TMapper functor;
                FunctorContainer results;
                for(std::string line; std::getline(file, line) && (file.tellg() <= offsetEnd);)
                {
                    FunctorContainer partResults = functor(line);
                    results.insert(results.end(), partResults.begin(), partResults.end());
                }

                //sort local vector
                std::sort(results.begin(), results.end());

                file.close();
                return results;
            },
            blockSize, i);
        }

        //wait for all map threads to finish
        for (auto &future : futures)
        {
            mapResults.push_back(future.get());
        }

        return mapResults;
    }

    auto shuffle(ResultsContainer &mapResults)
    {
        ResultsContainer shuffleResults(m_rnum);
        std::vector<std::future<void> > futures;
        std::vector<std::unique_ptr<std::mutex> > shuffleMutexes;
        for (size_t i = 0; i < m_rnum; ++i)
        {
            shuffleMutexes.push_back(std::make_unique<std::mutex>());
        }

        for (auto &values : mapResults)
        {
            futures.push_back(std::async([this, &shuffleResults, &shuffleMutexes, &values ]()
            {
                for(auto &value : values)
                {
                    int index = value[0] % m_rnum;

                    //thread safe insert
                    std::lock_guard<std::mutex> lock(*shuffleMutexes.at(index));

                    shuffleResults[index].push_back(std::move(value));
                }
            }));
        }

        //wait for all shuffle threads to finish
        for (auto &future : futures)
        {
            future.get();
        }

        //sort local vector
        for(auto &result : shuffleResults)
        {
            std::sort(result.begin(), result.end());
        }

        return shuffleResults;
    }

    auto reduce(ResultsContainer &shuffleResults)
    {
        ResultsContainer reduceResults;
        std::vector<std::future<FunctorContainer> > futures;

        for (auto &values : shuffleResults)
        {
            futures.push_back(std::async([this, &values]()
            {
                //exec user reduce functor
                TReducer functor;
                FunctorContainer results;
                for(auto &value : values)
                {
                    FunctorContainer partResults = functor(value);
                    results.insert(results.end(), partResults.begin(), partResults.end());
                }

                return results;
            }));
        }

        //wait for all reduce threads to finish
        for (auto &future : futures)
        {
            reduceResults.push_back(future.get());
        }

        return reduceResults;
    }

private:
    void moveOffsetToEndOfLine(std::ifstream &file, int &offset)
    {
        file.seekg(offset, std::ios_base::beg);

        //first line in file
        if (offset == 0)
            return;

        //iterate to '\n'
        while ((file.get() != 10) && (file.peek() != EOF));

        //prevent EOF (-1) on last line of file
        if (file.tellg() != EOF)
            offset = file.tellg();

        //renew 'tellg()' for right work
        if (file.tellg() == EOF)
            file.clear();
    }

private:
    std::string m_filePath;
    int m_mnum;
    int m_rnum;
};

//emails_middle.txt 8 8
int main(int argc, char * argv[])
{
    if (argc != 4)
    {
        std::cerr << "Usage: yamr <filePath> <mnum> <rnum>" << std::endl;
        return 1;
    }

    TextFileMapReduceFramework<MapFunctor, ReduceFunctor> framework (std::string(argv[1]), std::atoi(argv[2]), std::atoi(argv[3]));
    return framework.run( );
}
