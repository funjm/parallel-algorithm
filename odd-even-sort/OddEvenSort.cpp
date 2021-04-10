/*
 * @Version: 1.0
 * @Autor: Li dianchao
 * @Date: 2021-04-02 20:17:57
 * @LastEditors: Li dianchao
 * @LastEditTime: 2021-04-08 20:05:59
 */

#include <iostream>
#include <algorithm>
#include <random>
#include <time.h>
#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include <limits.h>
#include <assert.h>
#include <sys/time.h>
#include <unistd.h>
#include <malloc.h>
#include <memory.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <mpi.h>


#define BLOCK 1024 * 1024

using namespace std;

/*分发数据至各个进程*/
void Get_Input(float *A, int64_t local_n, int my_rank, int comm_sz, int64_t datasize, char *datafile, double *beginTime) {
    float* array = NULL;
    // 主进程分发数据
    if (my_rank == 0) {
        // array = new float[datasize];
        // srand((int)time(0));
        // for (int i = 0; i < datasize; i++) {
        //     array[i] = rand() * 0.2;
        // }
        // MPI_Scatter(array, local_n, MPI_FLOAT, A, local_n, MPI_FLOAT, 0, MPI_COMM_WORLD);
        // delete[] array;

        int handle = open(datafile, O_RDONLY);
        if(handle == -1){
            printf("Cannot Open file %s\n", datafile);
            return;
        }
        
        int64_t total = datasize; // 其中有不能整除问题，对于不能整除的进行填补
        if (datasize % comm_sz != 0) {
            total = ((datasize / comm_sz) + 1) * comm_sz;
        }

        array = (float*)malloc(sizeof(float) * total);
        read(handle, array, sizeof(float) * datasize);

        if (total - datasize > 0) {
            // array[i] = [0, 1]
            memset(array + datasize, 11111, sizeof(float) * (total - datasize));
        }

        // 起始时间
        *beginTime = MPI_Wtime();
        MPI_Scatter(array, local_n, MPI_FLOAT, A, local_n, MPI_FLOAT, 0, MPI_COMM_WORLD);
        close(handle);
        free(array);
    }
    // 其余进程接收数据
    else {
        MPI_Scatter(array, local_n, MPI_FLOAT, A, local_n, MPI_FLOAT, 0, MPI_COMM_WORLD);
    }
}

// 合并两个进程的数据，并取较小的一半数据
void Merge_Low(float *A, float *B, int64_t local_n) {
    float* temp = (float*)malloc(sizeof(float) * local_n);
    int64_t p_a = 0;
    int64_t p_b = 0;
    int64_t i = 0;
    // 从前往前面取最小值
    while (i < local_n) {
        if (A[p_a] < B[p_b]) {
            temp[i] = A[p_a];
            i++;
            p_a++;
        }
        else {
            temp[i] = B[p_b];
            i++;
            p_b++;
        }
    }
    memcpy(A, temp, sizeof(float) * local_n);
    free(temp);
}

/*合并两个进程的数据，并取较大的一半数据*/
void Merge_High(float *A, float *B, int64_t local_n) {
    float* temp = (float*)malloc(sizeof(float) * local_n);
    int64_t p_a = local_n - 1; 
    int64_t p_b = local_n - 1;
    int64_t i = local_n - 1;
    // 从后往前面取最大值
    while (i >= 0) {
        if (A[p_a] > B[p_b]) {
            temp[i] = A[p_a];
            i--;
            p_a--;
        }
        else {
            temp[i] = B[p_b];
            i--;
            p_b--;
        }
    }
    memcpy(A, temp, sizeof(float) * local_n);
    free(temp);
}

/*获得某个阶段，某个进程的通信伙伴*/
int Get_Partner(int my_rank, int phase) {
    // 偶交换
    if (phase % 2 == 0) {
        if (my_rank % 2 == 0) {
            return my_rank - 1;
        }
        else {
            return my_rank + 1;
        }
    }
    // 奇交换
    else {
        if (my_rank % 2 == 0) {
            return my_rank + 1;
        }
        else {
            return my_rank - 1;
        }
    }
}

/*收集排序数组*/
void Final_Sorted(float *A, int64_t local_n, int my_rank, int comm_sz, int64_t datasize, double *beginTime) {
    float* array = NULL;
    // 0号进程接收各个进程的数据
    if (my_rank == 0) {
        int64_t total = datasize; // 其中有不能整除问题，对于不能整除的进行填补
        if (datasize % comm_sz != 0) {
            total = ((datasize / comm_sz) + 1) * comm_sz;
        }
        array = (float*)malloc(sizeof(float) * total);
        MPI_Gather(A, local_n, MPI_FLOAT, array, local_n, MPI_FLOAT, 0, MPI_COMM_WORLD);
        // for (int i = 0; i < total; i++) {
        //     cout << array[i] << "\t";
        //     if (i % 4 == 3) {
        //         cout << endl;
        //     }
        // }
        // cout << endl;
        cout<<"data[0]="<<array[0]<<"  last data="<<array[datasize - 1]<<endl;

        // 结束时间
        double endTime = MPI_Wtime();
        printf("spent time = %lf second\n", endTime - *beginTime);
        free(array);
    }
    // 其余进程发送数据
    else {
        // 在MPI_Gather中，只有根进程需要一个有效的接收缓存。所有其他的调用进程可以传递NULL给recv_data。
        // 另外，别忘记recv_count参数是从每个进程接收到的数据数量，而不是所有进程的数据总量之和。这一点经常容易搞错。
        MPI_Gather(A, local_n, MPI_FLOAT, array, local_n, MPI_FLOAT, 0, MPI_COMM_WORLD);
    }
}

int cmp(const void *a, const void *b)
{
    return *((float *)a) > *((float *)b);
}

// 起始时间
double beginTime;
// double endTime;

int main(int argc, char **argv) {
    int64_t local_n;    // 各个进程中数组的大小
    float *A, *B;     // A为进程中保存的数据，B为进程通信中获得的数据
    int comm_sz, my_rank;
    
    MPI_Init(NULL, NULL);
    
    MPI_Comm_size(MPI_COMM_WORLD, &comm_sz);  
    MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
    
    // 计算每个进程应当获得的数据量
    int64_t datasize = atoll(argv[2]) * BLOCK;
    int64_t total = datasize; 
    // 其中有不能整除问题，对于不能整除的进行填补
    if (datasize % comm_sz != 0) {
        total = ((datasize / comm_sz) + 1) * comm_sz;
    }
    local_n = total / comm_sz;
    A = (float*)malloc(sizeof(float) * local_n);
    B = (float*)malloc(sizeof(float) * local_n);
    
    // 1. 初始化数据
    // 2. 把数据广播出去
    Get_Input(A, local_n, my_rank, comm_sz, datasize, argv[1], &beginTime);

    // 3. local sort
    qsort(A, local_n, sizeof(int), cmp);
    
    // 4. 进行np轮排序
    for (int i = 0; i < comm_sz; i++) {
        
        int partner = Get_Partner(my_rank, i);
        // 如果本次数据交换的进程号有效
        if (partner != -1 && partner != comm_sz) {
            // 交换数据
            MPI_Sendrecv(A, local_n, MPI_FLOAT, partner, 0, B, local_n, MPI_FLOAT, partner, 0, MPI_COMM_WORLD, MPI_STATUSES_IGNORE);
            // 取值较大的部分
            if (my_rank > partner) {
                Merge_High(A, B, local_n);
            }
            // 取值较小的部分
            else {
                Merge_Low(A, B, local_n);
            }
        }
    }
    
    // 5. 0号进程收集排序好的数据
    Final_Sorted(A, local_n, my_rank, comm_sz, datasize, &beginTime);
    
    MPI_Finalize();

    free(A);
    free(B);
    return 0;
}

