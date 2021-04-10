#include "mpi.h"
#include <cstdio>
#include <algorithm>
#include <cmath>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#define BLOCK 1024*1024LL
using namespace std;

typedef long long __int64;

int Compute_partner(int phase,int my_rank,int comm_sz){//根据趟数的奇偶性以及当前编号的编号得到partner进程的编号

    int partner;
	
    if(!(phase&1)){		//当前是偶数趟
        if(my_rank&1){	//本进程是奇数进程
            partner=my_rank-1; //与我前面的进程进行排序
        }
        else{
            partner=my_rank+1;
        }
    }
    else{				//当前是奇数趟
        if(my_rank&1){	//本进程是奇数进程
            partner=my_rank+1;
        }
        else{
            partner=my_rank-1;
        }
    }
    if(partner==-1 || partner==comm_sz){ //进程值不合法
        partner=MPI_PROC_NULL;
    }
    return partner;
}

int cmp(const void *a, const void *b)
{
    return *((float *)a) > *((float *)b);
}


int main(int argc, char* argv[]){
    int my_rank=0, comm_sz=0;
	
    MPI_Init(&argc, &argv);
	//获得当前进程号
    MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
	//获取进程总数
    MPI_Comm_size(MPI_COMM_WORLD, &comm_sz);
	
    int np;//奇偶排序的趟数
	int n ; //读入的数据总量
	int local_n;//分成的每段的长度

    int fp;
    if(my_rank==0){
        fp=open("1G_floats",O_RDONLY);
        if(fp==-1){
            printf("Cannot Open file \n");
            exit(1);
        } 
		
    }
	n = atoi(argv[1]) * BLOCK;
	np = comm_sz;
	local_n = n / comm_sz; //计算出每段的长度
	printf("local_n: %d\n",local_n);
	printf("np: %d\n",np);
	/*
	MPI_Bcast(&np,1,MPI_INT,0,MPI_COMM_WORLD);
    MPI_Bcast(&local_n,1,MPI_INT,0,MPI_COMM_WORLD);
	*/
    // float* keys = new float[n];
    float* keys = NULL;
    float* my_keys=new float[local_n];
	
    if(my_rank==0){	//root读取全部的数据
        printf("n : %d\n", n);
        keys = new float[n];
        read(fp,keys,n*sizeof( float));
        printf("%f\n",keys[0]);
        printf("%f\n",keys[n-1]);
        close(fp);
    
    }
	//分keys数组到每个进程的my_keys上，每个上面的长度为local_n
	MPI_Scatter(keys,local_n,MPI_FLOAT,my_keys,local_n,MPI_FLOAT,0,MPI_COMM_WORLD);
    
    
	//计时开始
    double beginTime = MPI_Wtime();
    qsort(my_keys,local_n, sizeof(float), cmp);//串行快速排序
    float* recv_keys = new float[local_n];
    float* temp_keys = new float[local_n];
	
    for(int i = 0;i<np;++i){	//循环np趟数
		printf("%d \n", i);
        int partner = Compute_partner(i, my_rank, comm_sz);//得到要交换数据进程的编号
		
        if (partner != MPI_PROC_NULL){ 	//进程号合法
		
			//将当前的my_keys发送给partner进程，并用recv_key接收来自partner进程的keys。
            MPI_Sendrecv(my_keys, local_n, MPI_FLOAT, partner, 0, recv_keys, local_n, MPI_FLOAT, partner, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
			
            if(my_rank<partner){//编号小的进程留下归并时较小的一半
                int e=0,e1=0,e2=0;
                float* temp_keys=new float[local_n];
                while(e<local_n){
                    if(my_keys[e1]<=recv_keys[e2]){
                        temp_keys[e]=my_keys[e1];
                        ++e;
                        ++e1;
                    }
                    else{
                        temp_keys[e]=recv_keys[e2];
                        ++e;
                        ++e2;
                    }
                }
                for(int j=0;j<local_n;++j){
                    my_keys[j]=temp_keys[j];
                }
            }
            else{//编号大的进程留下归并时较大的一半
                int e=local_n-1,e1=local_n-1,e2=local_n-1;
                while(e>=0){
                    if(my_keys[e1]>=recv_keys[e2]){
                        temp_keys[e]=my_keys[e1];
                        --e;
                        --e1;
                    }
                    else{
                        temp_keys[e]=recv_keys[e2];
                        --e;
                        --e2;
                    }
                }
                for(int j=0;j<local_n;++j){
                    my_keys[j]=temp_keys[j];
                }
            }
        }
    }
	//计时结束
    double endTime = MPI_Wtime();
	
	double costTime = endTime - beginTime;
	
	//整合全部进程的my_keys数组到 root的keys数组
	MPI_Gather(my_keys, local_n, MPI_FLOAT, keys, local_n, MPI_FLOAT, 0, MPI_COMM_WORLD);
	
	
    FILE * result ;
    result = fopen("result.txt","w+");
    if (my_rank == 0){ //root节点输出排序值，和时间
		printf("write ...");
        /*for(int i=0;i<n;++i){
            fprintf(result,"%f ",keys[i]);
        }*/
        puts("");
		printf("spent time = %lf second\n", costTime);
        
    }
    fclose(result);
	
	//回收与释放
    delete[] keys;
    delete[] my_keys;
    delete[] recv_keys;
    delete[] temp_keys;
    MPI_Finalize();
	
	
    exit(0);
}