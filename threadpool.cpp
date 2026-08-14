#include<iostream>
#include<thread>
#include<vector>
#include<queue>
#include<mutex>
#include<functional>
#include<condition_variable>
#include<string>
#include<chrono>
#include<unordered_map>
using namespace std;
struct TaskNode {
    int priority;
    uint64_t id;
    function<void()> func;

    // priority_queue 默认是最大堆，用 > 实现最小堆（数值越小越先执行）
    bool operator<(const TaskNode& other) const {
        return priority > other.priority;
    }
};


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
                        //tasks.top()返回的是const TaskNode&，需要去const
                        TaskNode task=move(const_cast<TaskNode&>(tasks.top()));
                        tasks.pop();
                        lock.unlock();
                        bool cancelled = false;
                        {
                            std::lock_guard<std::mutex> lock(cancel_mtx_);
                            auto it = cancelmap.find(task.id);
                            if (it != cancelmap.end()) {
                                cancelled = it->second;
                                cancelmap.erase(it); 
                            } 
                        }if (cancelled) {
                                continue; 
                            }
                        task.func();
                    }
                }
            );
        }


    }
    template<class F, class ...ARG>
    void addtask(int priority, F&& f, ARG && ...arg) {
        uint64_t id = ++next_id_;
        {unique_lock<mutex>canslock(cancel_mtx_);
        cancelmap[id] = false;
        }
        
        //别写成forward<ARG>((arg)...)
        function<void()>task = bind(forward<F>(f), forward<ARG>(arg)...);
        unique_lock<mutex>lock(mtx);
        tasks.emplace(TaskNode{ priority, id, move(task)});
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
    bool cancel(uint64_t id) {
        std::lock_guard<std::mutex> lock(cancel_mtx_);
        auto it = cancelmap.find(id);
        if (it != cancelmap.end()) {
            it->second = true;   // 标记为取消
            return true;
        }
        return false;
    }
private:
    // 成员变量
    priority_queue<TaskNode> tasks;
    unordered_map<uint64_t, bool> cancelmap;  // 只存 ID -> 是否取消
    mutex cancel_mtx_;  // 保护 cancel_map_
    uint64_t next_id_ = 0;
    vector<thread>workers;
    //缺()
    condition_variable condition;
    mutex mtx;
    bool stop;

};
//忘加;
int main() {
    threadpool pool(6);
    for (int i = 1; i <= 10; i++)
    {
        pool.addtask(i%2,[i] {
            printf("task %d is ok\n", i);
            this_thread::sleep_for(chrono::seconds(4));
            printf("task %d is done\n", i);
            });
    }
    pool.cancel(3);
    return 0;
}