#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <sys/time.h>
#include <stddef.h>
#include <stdbool.h>

struct matrix_element 
{
	char matrix;
    int row;
    int col;
    int value;
};
struct key
{
	int i;
	int k;
};
struct value
{
	char matrix;
	int j;
	int val;
};

struct key_value
{
	struct key k;
	struct value v;
};

int main(int argc, char **argv)
{
	int N = 25;
	int n = N / 5;
	int buffer_size;
	int rank = 0;
	MPI_Init(&argc, &argv);
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);

	if (rank == 0)
	{
		MPI_Datatype mystruct_type;
        MPI_Datatype types[] = { MPI_CHAR, MPI_INT, MPI_INT , MPI_INT  };
        int blocklengths[] = {1,1,1,1};
        MPI_Aint displacements[] = { offsetof(struct matrix_element,matrix), offsetof(struct matrix_element,row),offsetof(struct matrix_element,col), offsetof(struct matrix_element,value) };
        MPI_Type_create_struct(4, blocklengths, displacements, types, &mystruct_type);
        MPI_Type_commit(&mystruct_type);


		FILE *fp2 = fopen("input.txt","r"); // Open file for reading
		if (fp2 == NULL) 
		{
			return 1;
		}

		int line_count=0;
		char ch; 
		while((ch=fgetc(fp2))!=EOF) 
		{ 
			if (ch == '\n') 
			{
				line_count++;
			}
		}
		fclose(fp2); // Close file
		int chunk=line_count/5;
		int current_chunk_range=chunk;
		int num=0;
		int current_mapper_rank=1;
		struct matrix_element element[chunk*2];
		int total=0;

		FILE *fp = fopen("input.txt", "r");
		while (!feof(fp)) 
		{
			int row, col;
			double value;
			char matrix_name;
			if (fscanf(fp, "%c,%d,%d,%lf\n",&matrix_name,&row,&col,&value) == 4) 
			{
				element[num].row = row;
				element[num].col = col;
				element[num].value = value;
				if (matrix_name == 'A') 
				{
					element[num].matrix='A';
				}
				else
				{
					element[num].matrix='B';
				}
				num++;
				if(num==chunk-1 && current_mapper_rank!=5)
				{
					MPI_Send(&num,1,MPI_INT,current_mapper_rank,0,MPI_COMM_WORLD);
					MPI_Send(element,num,mystruct_type,current_mapper_rank,0,MPI_COMM_WORLD);
					total=total+num;
					current_mapper_rank++;
					num=0;
				}
			} 
    	}
		fclose(fp);
		MPI_Send(&num,1,MPI_INT,current_mapper_rank,0,MPI_COMM_WORLD);
		MPI_Send(element,num,mystruct_type,current_mapper_rank,0,MPI_COMM_WORLD);

		MPI_Type_free(&mystruct_type);

		MPI_Datatype my_type1; // Define MPI datatype for struct
		MPI_Datatype types1[] = { MPI_INT, MPI_INT, MPI_CHAR, MPI_INT, MPI_INT };
		int block_lengths1[] = { 1, 1, 1, 1, 1 };
		MPI_Aint displacements1[] = { offsetof(struct key_value, k.i), offsetof(struct key_value, k.k), offsetof(struct key_value, v.matrix), offsetof(struct key_value, v.j), offsetof(struct key_value, v.val) };
		MPI_Type_create_struct(5, block_lengths1, displacements1,types1, &my_type1);
		MPI_Type_commit(&my_type1);

		struct key_value kv;

		int arr[atoi(argv[1])][atoi(argv[1])];
		while (1) 
		{
			MPI_Status status;
			MPI_Recv(&kv,1,my_type1,7,MPI_ANY_TAG,MPI_COMM_WORLD, &status);
			arr[kv.k.i][kv.k.k]=kv.v.val;
			if(status.MPI_TAG==21)
			{
				break;
			}
		}
		printf("\nFinal Output:\n");
		for(int i=0;i<atoi(argv[1]);i++)
		{
			for(int j=0;j<atoi(argv[1]);j++)
			{
				printf("i: %d , j: %d , value: %d\n",i,j,arr[i][j]);
			}
		}

		MPI_Type_free(&my_type1);

		// total=total+num;
		// printf("%d %d %d %d \n",num,current_mapper_rank,chunk,total);

		}
	if (rank != 0 && rank <= 5)
	{
		printf("Task Map Assigned to process: %d\n", rank);

		MPI_Datatype mystruct_type;
        MPI_Datatype types[] = { MPI_CHAR, MPI_INT, MPI_INT , MPI_INT  };
        int blocklengths[] = {1,1,1,1};
        MPI_Aint displacements[] = { offsetof(struct matrix_element,matrix), offsetof(struct matrix_element,row),offsetof(struct matrix_element,col), offsetof(struct matrix_element,value) };
        MPI_Type_create_struct(4, blocklengths, displacements, types, &mystruct_type);
        MPI_Type_commit(&mystruct_type);

		int number_amount=0;
		MPI_Recv(&number_amount,1, MPI_INT,0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
		// printf("%d\n",number_amount);

		struct matrix_element element[number_amount];
		MPI_Recv(element,number_amount,mystruct_type,0,0,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
		// printf("%d %d %d %c\n",element[number_amount-1].col,element[number_amount-1].row,element[number_amount-1].value,element[number_amount-1].matrix);

		printf("Process %d received task Map on Master/Slave\n",rank);

		MPI_Datatype my_type1; // Define MPI datatype for struct
		MPI_Datatype types1[] = { MPI_INT, MPI_INT, MPI_CHAR, MPI_INT, MPI_INT };
		int block_lengths1[] = { 1, 1, 1, 1, 1 };
		MPI_Aint displacements1[] = { offsetof(struct key_value, k.i), offsetof(struct key_value, k.k), offsetof(struct key_value, v.matrix), offsetof(struct key_value, v.j), offsetof(struct key_value, v.val) };
		MPI_Type_create_struct(5, block_lengths1, displacements1,types1, &my_type1);
		MPI_Type_commit(&my_type1);

		struct key_value kv;
		int tag=0;

		for(int i=0;i<number_amount;i++)
		{
			if(element[i].matrix=='A')
			{
				for(int k=0;k<atoi(argv[1]);k++)
				{
					if(i==number_amount-1 && k==atoi(argv[1])-1)
						tag=rank;
					kv.k.i=element[i].row;
					kv.k.k=k;
					kv.v.j=element[i].col;
					kv.v.val=element[i].value;
					kv.v.matrix='A';
					// printf("%d %d %d %c\n",element[i].row,element[i].col,element[i].value,element[i].matrix);

					if(rank==1 || rank==3 || rank==5)
					{
						MPI_Send(&kv,1,my_type1,6,tag,MPI_COMM_WORLD);
					}
					else
					{
						MPI_Send(&kv,1, my_type1,7,tag,MPI_COMM_WORLD);
					}
				}
			}
			else
			{
				for(int k=0;k<atoi(argv[1]);k++)
				{
					if(i==number_amount-1 && k==atoi(argv[1])-1)
						tag=rank;
					kv.k.i=k;
					kv.k.k=element[i].col;
					kv.v.j=element[i].row;
					kv.v.val=element[i].value;
					kv.v.matrix='B';
					// printf("%d %d %d %c\n",element[i].row,element[i].col,element[i].value,element[i].matrix);

					if(rank==1 || rank==3 || rank==5)
					{
						MPI_Send(&kv,1,my_type1,6,tag,MPI_COMM_WORLD);
					}
					else
					{
						MPI_Send(&kv,1,my_type1,7,tag,MPI_COMM_WORLD);
					}
				}
			}
		}
		printf("Process %d has completed task map \n", rank);

		MPI_Type_free(&mystruct_type);
		MPI_Type_free(&my_type1);
	}
	if(rank==6 || rank==7)
	{
		printf("Task Reduce Assigned to process: %d\n", rank);

		MPI_Datatype my_type1; // Define MPI datatype for struct
		MPI_Datatype types1[] = { MPI_INT, MPI_INT, MPI_CHAR, MPI_INT, MPI_INT };
		int block_lengths1[] = { 1, 1, 1, 1, 1 };
		MPI_Aint displacements1[] = { offsetof(struct key_value, k.i), offsetof(struct key_value, k.k), offsetof(struct key_value, v.matrix), offsetof(struct key_value, v.j), offsetof(struct key_value, v.val) };
		MPI_Type_create_struct(5, block_lengths1, displacements1,types1, &my_type1);
		MPI_Type_commit(&my_type1);

		int check[6]={0,0,0,0,0,0};

		struct key_value hash_map[1000][100];

		for(int i=0;i<1000;i++)
		{
			for(int j=0;j<100;j++)
			{
				hash_map[i][j].v.val=-1;
			}
		}

		int hash_len=0;
		struct key_value kv;
		printf("Process %d received task Reduce on Master/Slave\n",rank);
		while (1) 
		{
			MPI_Status status;

			MPI_Recv(&kv,1,my_type1,MPI_ANY_SOURCE,MPI_ANY_TAG,MPI_COMM_WORLD, &status);
			// printf("%d %d %d %d %c\n",kv.k.i,kv.k.k,kv.v.j,kv.v.val,kv.v.matrix);
			// printf("%d %d %d %c\n",kv.v.j,element[i].col,element[i].value,element[i].matrix);

			if(hash_len==0)
			{
				hash_map[hash_len][0].k.i=kv.k.i;
				hash_map[hash_len][0].k.k=kv.k.k;
				hash_map[hash_len][0].v.j=kv.v.j;
				hash_map[hash_len][0].v.matrix=kv.v.matrix;
				hash_map[hash_len][0].v.val=kv.v.val;
				hash_len++;
			}
			else
			{
				bool isAlreadyPresent=false;
				for(int i=0;i<hash_len;i++)
				{
					if(hash_map[i][0].k.i==kv.k.i && hash_map[i][0].k.k==kv.k.k)
					{
						isAlreadyPresent=true;
						for(int j=0;j<100;j++)
						{
							if(hash_map[i][j].v.val==-1)
							{
								hash_map[i][j].k.i=kv.k.i;
								hash_map[i][j].k.k=kv.k.k;
								hash_map[i][j].v.j=kv.v.j;
								hash_map[i][j].v.matrix=kv.v.matrix;
								hash_map[i][j].v.val=kv.v.val;
								// if(hash_map[i][j].v.matrix=='B')
								// printf("%d %d %d %d %d %d %c\n",i,j,hash_map[i][j].k.i,hash_map[i][j].k.k,hash_map[i][j].v.j,hash_map[i][j].v.val,hash_map[i][j].v.matrix);
								break;
								
							}
						}
					}
					if(isAlreadyPresent==true)
						break;
				}
				if(isAlreadyPresent==false)
				{
					hash_map[hash_len][0].k.i=kv.k.i;
					hash_map[hash_len][0].k.k=kv.k.k;
					hash_map[hash_len][0].v.j=kv.v.j;
					hash_map[hash_len][0].v.matrix=kv.v.matrix;
					hash_map[hash_len][0].v.val=kv.v.val;
					hash_len++;
				}
			}
			check[status.MPI_TAG]=1;

			if(check[1]==1 && check[3]==1 && check[5]==1 && rank==6)
			{
				break;
			}
			if(check[2]==1 && check[4]==1 && rank==7)
			{
				break;
			}

		}
		for(int i=0;i<hash_len;i++)
		{
			int *hashmap_A= (int*) malloc(1000 * sizeof(int));
			int *hashmap_B= (int*) malloc(1000 * sizeof(int));

			for(int j=0;j<1000;j++)
			{
				hashmap_A[j]=0;
				hashmap_B[j]=0;
			}
			for(int j=0;j<1000;j++)
			{
				// printf("%d ",hash_map[i][j].v.val);
				if(hash_map[i][j].v.matrix=='A')
				{
					// hashmap_A[index1++]=hash_map[i][j].v.val;
					hashmap_A[hash_map[i][j].v.j]=hash_map[i][j].v.val;
					// printf("%d\n",hashmap_A[hash_map[i][j].v.j]);
				}
				else
				{
					// hashmap_B[index2++]=hash_map[i][j].v.val;
					hashmap_B[hash_map[i][j].v.j]=hash_map[i][j].v.val;
					// printf("%d\n",hashmap_B[hash_map[i][j].v.j]);
				}
			}
			if(rank==6)
			{
				MPI_Send(hashmap_A,1000,MPI_INT,7,10,MPI_COMM_WORLD);
				MPI_Send(hashmap_B,1000,MPI_INT,7,11,MPI_COMM_WORLD);
			}
			else
			{
				int *hashmap_A1= (int*) malloc(1000 * sizeof(int));
				int *hashmap_B1= (int*) malloc(1000 * sizeof(int));

				MPI_Recv(hashmap_A1,1000, MPI_INT,6,10,MPI_COMM_WORLD, MPI_STATUS_IGNORE);
				MPI_Recv(hashmap_B1,1000,MPI_INT,6,11,MPI_COMM_WORLD, MPI_STATUS_IGNORE);

				for(int j=0;j<1000;j++)
				{
					if(hashmap_A1[j]!=-1 && hashmap_A1[j]!=0)
					{
						hashmap_A[j]=hashmap_A1[j];
					}
					if(hashmap_B1[j]!=-1 && hashmap_B1[j]!=0)
					{
						hashmap_B[j]=hashmap_B1[j];
					}
				}
				int result=0;
				for(int j=0;j<1000;j++)
				{
					if(hashmap_A[j]!=-1 && hashmap_B[j]!=-1)
					{
						// printf("%d\n",hashmap_A[j]);
						result+=hashmap_A[j]*hashmap_B[j];
					}
				}
				// printf("%d %d %d\n",hash_map[i][0].k.i,hash_map[i][0].k.k,result);

				MPI_Datatype my_type2; // Define MPI datatype for struct
				MPI_Datatype types2[] = { MPI_INT, MPI_INT, MPI_CHAR, MPI_INT, MPI_INT };
				int block_lengths2[] = { 1, 1, 1, 1, 1 };
				MPI_Aint displacements2[] = { offsetof(struct key_value, k.i), offsetof(struct key_value, k.k), offsetof(struct key_value, v.matrix), offsetof(struct key_value, v.j), offsetof(struct key_value, v.val) };
				MPI_Type_create_struct(5, block_lengths2, displacements2,types2, &my_type2);
				MPI_Type_commit(&my_type2);

				struct key_value kv1;
				kv1.k.i=hash_map[i][0].k.i;
				kv1.k.k=hash_map[i][0].k.k;
				kv1.v.val=result;
				if(i==hash_len-1)
					MPI_Send(&kv1,1,my_type2,0,21,MPI_COMM_WORLD);
				else
					MPI_Send(&kv1,1,my_type2,0,20,MPI_COMM_WORLD);
				MPI_Type_free(&my_type2);
			}
			// printf("\n");
			// return 0;
		}
		MPI_Type_free(&my_type1);
		printf("Process %d has completed task reduce \n", rank);
	}
	MPI_Finalize();
	return 0;
}
