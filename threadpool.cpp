#include<iostream>
#include<thread>
#include<vector>
#include<queue>
#include<mutex>
#include<functional>
#include<condition_variable>
#include<string>
#include<chrono>
using namespace std;
class threadpool {
public://缺public
    threadpool(int workernum) {
        stop = false;
        for (int i = 0; i < workernum; i++)
        {
            workers.emplace_back(
                [this]() {
                    while (1) {
                        unique_lock<mutex>lock(mtx);
                        //睡眠时自动释放锁，唤醒后重新抢回锁
                        //this能捕获当前对象的指针，比&或=更好
                        condition.wait(lock, [this] {
                            //返回0时睡眠
                            return stop || !tasks.empty();
                            });
                        if (stop && tasks.empty())return;
                        //move可以避免昂贵的拷贝
                        function<void()>task(move(tasks.front()));
                        tasks.pop();
                        lock.unlock();
                        task();
                    }
                }
            );
        }


    }
    template<class F, class ...ARG>
    void addtask(F&& f, ARG && ...arg) {
        //别写成forward<ARG>((arg)...)
        function<void()>task = bind(forward<F>(f), forward<ARG>(arg)...);
        unique_lock<mutex>lock(mtx);
        tasks.emplace(move(task));
        lock.unlock();
        condition.notify_one();

    }
    ~threadpool() {
        unique_lock<mutex>lock(mtx);
        stop = true;
        lock.unlock();
        condition.notify_all();
        for (auto& i : workers)
        {
            if (i.joinable())
            {
                i.join();
            }

        }


    }
private:
    vector<thread>workers;
    //缺()
    queue<function<void()>>tasks;
    condition_variable condition;
    mutex mtx;
    bool stop;

};
//忘加;
int main() {
    threadpool pool(6);
    for (int i = 0; i < 10; i++)
    {
        pool.addtask([i] {
            printf("task %d is ok\n", i);
            this_thread::sleep_for(chrono::seconds(4));
            printf("task %d is done\n", i);
            });
    }
    
    return 0;
}