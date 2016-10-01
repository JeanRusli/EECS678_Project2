/** @file libscheduler.c
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "libscheduler.h"
#include "../libpriqueue/libpriqueue.h"

/**
  Stores information making up a job to be scheduled including any statistics.

  You may need to define some global variables or a struct to store your job queue elements. 
*/
typedef struct _job_t
{
	int id;			// ID is unique, based on arrival time.
	int arr_time; 		// Arrival time.
	int duration;		// Total running time.
	int priority;	

	int remaining_time;	// Used in SJF and preemptive scheduler.
	int wait_time;		
	int start_wait;		// Used in preemptive scheduler.
	int response_time;
	int turnover_time;

	int turn;		// Used in Round Robin.

	int core_id;
	struct _job_t *next;	// Pointer to next job in the list.
} job_t;

int numOfJobs;
int totalJobs;
int numOfCores;
job_t *job_ptr;		// Head of job list.
int *core_arr;		// Status of core array.
priqueue_t *head;	// Head of queue.
scheme_t sch;		// Scheduler type.

int compare1(const void * a, const void * b)
{
	return ( ((job_t*)a)->id - ((job_t*)b)->id );
}

int compare2(const void * a, const void * b)
{
	if( ((job_t*)a)->remaining_time < ((job_t*)b)->remaining_time ) {
		return -1;
	} else if( ((job_t*)a)->remaining_time > ((job_t*)b)->remaining_time ) {
		return 1;
	} else return ( ((job_t*)a)->id - ((job_t*)b)->id );
}

int compare3(const void * a, const void * b)
{
	if( ((job_t*)a)->priority < ((job_t*)b)->priority ) {
		return -1;
	} else if( ((job_t*)a)->priority > ((job_t*)b)->priority ) {
		return 1;
	} else return ( ((job_t*)a)->id - ((job_t*)b)->id );
}

int compare4(const void * a, const void * b)
{
	return ( ((job_t*)a)->turn - ((job_t*)b)->turn );
}

/**
  Initalizes the scheduler.
 
  Assumptions:
    - You may assume this will be the first scheduler function called.
    - You may assume this function will be called once.
    - You may assume that cores is a positive, non-zero number.
    - You may assume that scheme is a valid scheduling scheme.

  @param cores the number of cores that is available by the scheduler. These cores will be known as core(id=0), core(id=1), ..., core(id=cores-1).
  @param scheme the scheduling scheme that should be used. This value will be one of the six enum values of scheme_t
*/
void scheduler_start_up(int cores, scheme_t scheme)
{
	// Initialize variables.
	int i;
	numOfJobs = 0;
	totalJobs = 0;
	job_ptr = NULL;
	numOfCores = cores;

	// Allocate and initialize core array.
	core_arr = malloc(cores * sizeof(int));
	for(i = 0; i < cores; i++) {
		core_arr[i] = 0;
	}

	// Allocate the queue.
	head = malloc(sizeof(priqueue_t));
	
	// Initialize queue and pass the comparer function.
	sch = scheme;
	switch(sch) {

		case FCFS  :	
			priqueue_init(head, compare1);
			break;
		case SJF  :	
		case PSJF  :	
			priqueue_init(head, compare2);
			break;
		case PRI  :	
		case PPRI  :	
			priqueue_init(head, compare3);
			break;
		case RR  :	
			priqueue_init(head, compare4);
			break;
		default :	
			priqueue_init(head, compare1);

	}
}


/**
  Called when a new job arrives.
 
  If multiple cores are idle, the job should be assigned to the core with the
  lowest id.
  If the job arriving should be scheduled to run during the next
  time cycle, return the zero-based index of the core the job should be
  scheduled on. If another job is already running on the core specified,
  this will preempt the currently running job.
  Assumptions:
    - You may assume that every job wil have a unique arrival time.

  @param job_number a globally unique identification number of the job arriving.
  @param time the current time of the simulator.
  @param running_time the total number of time units this job will run before it will be finished.
  @param priority the priority of the job. (The lower the value, the higher the priority.)
  @return index of core job should be scheduled on
  @return -1 if no scheduling changes should be made. 
 
 */
int scheduler_new_job(int job_number, int time, int running_time, int priority)
{	
	// Declare, initialize, and update variables.	
	int i;	
	int scheduled = 0;
	numOfJobs++;
	totalJobs++;
	job_t *tmp1;
	job_t *temp;
	
	// Add new job to the job list.
	if(job_number == 0) {
		job_ptr = malloc(sizeof(job_t));
		tmp1 = job_ptr;
	} else if(job_number == 1) {
		tmp1 = malloc(sizeof(job_t));
		job_ptr->next = tmp1;
	} else {
		tmp1 = job_ptr;
		for(i = 0; i < (totalJobs-2); i++) {		
			tmp1 = tmp1->next;
		}
		temp = malloc(sizeof(job_t));
		tmp1->next = temp;
		tmp1 = temp;
	}
	
	// Initialize the new job.
	tmp1->id = job_number;
	tmp1->arr_time = time;
	tmp1->duration = running_time;
	tmp1->priority = priority;

	tmp1->remaining_time = running_time;
	tmp1->wait_time = -1;
	tmp1->start_wait = -1;
	tmp1->response_time = -1;
	tmp1->turnover_time = -1;

	tmp1->turn = -1;

	tmp1->core_id = -1;
	tmp1->next = NULL;
	
	int new_core_id = -1;
	int index = -1;

	// Update the remaining time of jobs that are currently active.
	for(i = 0; i < priqueue_size(head); i++) {
		job_t *tmp2 = (job_t*)priqueue_at(head,i);
		if(tmp2->core_id != -1) {
			tmp2->remaining_time = tmp2->duration - (time -  tmp2->wait_time - tmp2->arr_time);
		}
	}

	// Check if some cores are idle. If a core is idle, schedule the new job on that core.
	// Update the job variables and insert it into the queue. Update core array.
	for(i = 0; i < numOfCores; i++) {
		if(core_arr[i] == 0) {
			new_core_id = i;
			tmp1->core_id = i;
			tmp1->wait_time = 0;
			tmp1->response_time = 0;
			if(sch == RR) {
				tmp1->turn = priqueue_size(head);
			}
			index = priqueue_offer(head, tmp1);
			core_arr[i] = 1;
			scheduled = 1;
			break;
		}
	}
	
	// If all cores are currently busy, check if they can be preempted (only for PSJF and PPRI).
	// If a core can be preempted, schedule the new job on that core.
	// Update the old job variables before setting it as inactive.
	// Then, update the new job variables.
	if(scheduled == 0 && (sch == PSJF || sch == PPRI)) {
		index = priqueue_offer(head, tmp1);
		if(index < numOfCores) {
			for(i = (priqueue_size(head)-1); i >= 0; i--) {
				if(((job_t*)priqueue_at(head,i))->core_id != -1) {
					job_t *tmp2 = (job_t*)priqueue_at(head,i);
					new_core_id = tmp2->core_id;
					core_arr[new_core_id] = 1;
					tmp2->core_id = -1;
					tmp2->start_wait = time; 
					tmp2->remaining_time = tmp2->duration - (time -  tmp2->wait_time - tmp2->arr_time);
					// Check if the current active job has run any cycle. If it hasn't, return 
					// the variables to default value.
					if(tmp2->remaining_time == tmp2->duration) {
						tmp2->response_time = -1;
						tmp2->start_wait = -1; 
						tmp2->wait_time = -1; 
					}
					break;
				}
			}
			tmp1->core_id = new_core_id;
			tmp1->wait_time = 0;
			tmp1->response_time = 0;
			scheduled = 1;
		}
	}
	
	// If it can't be scheduled right away, just insert it into the queue.
	if(scheduled == 0 && sch != PSJF && sch != PPRI) {
		if(sch == RR) {
			tmp1->turn = priqueue_size(head);
		}
		index = priqueue_offer(head, tmp1);	
	}

	// Return the core id of the new job if scheduled. Otherwise, return -1.
	return new_core_id;
}


/**
  Called when a job has completed execution.
 
  The core_id, job_number and time parameters are provided for convenience. You may be able to calculate the values with your own data structure.
  If any job should be scheduled to run on the core free'd up by the
  finished job, return the job_number of the job that should be scheduled to
  run on core core_id.
 
  @param core_id the zero-based index of the core where the job was located.
  @param job_number a globally unique identification number of the job.
  @param time the current time of the simulator.
  @return job_number of the job that should be scheduled to run on core core_id
  @return -1 if core should remain idle.
 */
int scheduler_job_finished(int core_id, int job_number, int time)
{
	// Declare and update variables.
	numOfJobs--;
	int i; 
	job_t *tmp;
	int idle_core_id;
	core_arr[core_id] = 0;	// Update core array.
	
	// Check if scheme is Round Robin.
	if(sch == RR) {
		// for-loop to find the finished job in the list.
		// Update that job's variables and remove it from queue.
		for(i = 0; i < priqueue_size(head); i++) {
			tmp = (job_t*)priqueue_at(head,i);
			if(tmp->id == job_number) {
				tmp->turnover_time = time - tmp->arr_time;
				tmp->remaining_time = tmp->duration - (time - tmp->wait_time - tmp->arr_time);
				tmp->core_id = -1;
				idle_core_id = core_id;
				tmp->turn = -1;
				tmp = (job_t*)priqueue_remove_at(head,i);
			}
		}
		// Update the jobs' "turn" values.
		for(i = 0; i < priqueue_size(head); i++) {
			tmp = (job_t*)priqueue_at(head,i);
			tmp->turn = i;
		}
		// Get the next inactive job in queue. If it's empty, return -1.
		// Otherwise, return the new job id after its variables are updated.
		tmp = (job_t*)priqueue_peek(head);
		if(tmp == NULL) {
			return -1;
		} else {
			i = 0;
			while(i < (priqueue_size(head)-1) && tmp->core_id != -1) {
				i++;
				tmp = (job_t*)priqueue_at(head,i);
			}
			if(tmp->core_id != -1) {
				return -1;
			}
			if(tmp->response_time == -1) {		
				tmp->response_time = time - tmp->arr_time;
				tmp->wait_time = time - tmp->arr_time;
			}
			if(tmp->start_wait != -1) {
				tmp->wait_time += (time - tmp->start_wait);
				tmp->start_wait = -1;
			}
			tmp->core_id = idle_core_id;
			core_arr[core_id] = 1;		// Update core array.
			return tmp->id;
		}
	} else {
		// for-loop to find the finished job in the list.
		// Update that job's variables and remove it from queue.
		for(i = 0; i < priqueue_size(head); i++) {
			tmp = (job_t*)priqueue_at(head,i);
			if(tmp->id == job_number) {
				tmp->turnover_time = time - tmp->arr_time;
				tmp->remaining_time = tmp->duration - (time - tmp->wait_time - tmp->arr_time);
				tmp->core_id = -1;
				idle_core_id = core_id;
				tmp = (job_t*)priqueue_remove_at(head,i);
			}
		}

		// Get the next inactive job in queue. If it's empty, return -1.
		// Otherwise, return the new job id after its variables are updated.
		tmp = (job_t*)priqueue_peek(head);
		if(tmp == NULL) {
			return -1;
		} else {
			i = 0;
			while(i < (priqueue_size(head)-1) && tmp->core_id != -1) {
				i++;
				tmp = (job_t*)priqueue_at(head,i);
			}
			if(tmp->core_id != -1) {
				return -1;
			}
			if(tmp->response_time == -1) {
				tmp->response_time = time - tmp->arr_time;
				tmp->wait_time = time - tmp->arr_time;
			}
			if(tmp->start_wait != -1) {
				tmp->wait_time += (time - tmp->start_wait);
				tmp->start_wait = -1;
			}
			tmp->core_id = idle_core_id;
			core_arr[core_id] = 1;
			return tmp->id;
		}
	}
}


/**
  When the scheme is set to RR, called when the quantum timer has expired
  on a core.
 
  If any job should be scheduled to run on the core free'd up by
  the quantum expiration, return the job_number of the job that should be
  scheduled to run on core core_id.

  @param core_id the zero-based index of the core where the quantum has expired.
  @param time the current time of the simulator. 
  @return job_number of the job that should be scheduled on core cord_id
  @return -1 if core should remain idle
 */
int scheduler_quantum_expired(int core_id, int time)
{
	// Declare variables.
	job_t *curr;
	job_t *tmp;
	int i, index;

	// for-loop to get the job where the quantum has expired.
	// Set the job as inactive and update its variables.
	// Remove it from queue.
	for(i = 0; i < priqueue_size(head); i++) {
		curr = (job_t*)priqueue_at(head,i);
		if(curr->core_id == core_id) {
			core_arr[core_id] = -1;
			curr->core_id = -1;
			curr->start_wait = time; 
			curr->remaining_time = curr->duration - (time -  curr->wait_time - curr->arr_time);
			curr = (job_t*)priqueue_remove_at(head,i);
			curr->turn = priqueue_size(head);
			break;
		}
	}

	// Update the other jobs' "turn" values.
	for(i = 0; i < priqueue_size(head); i++) {
		tmp = (job_t*)priqueue_at(head,i);
		tmp->turn = i;
	}

	// Insert the job again into the queue.
	// With the "turn" variable updated, it will now be at the back of the queue.
	index = priqueue_offer(head, curr);
	
	// Get the next job in queue. If it's empty, return -1.
	// Otherwise, return the new job id after its variables are updated.
	tmp = (job_t*)priqueue_peek(head);
	if(tmp == NULL) {
		return -1;
	} else {
		i = 0;
		while(i < (priqueue_size(head)-1) && tmp->core_id != -1) {
			i++;
			tmp = (job_t*)priqueue_at(head,i);
		}
		if(tmp->core_id != -1) {
			return -1;
		}
		if(tmp->response_time == -1) {
			tmp->response_time = time - tmp->arr_time;
			tmp->wait_time = time - tmp->arr_time;
		}
		if(tmp->start_wait != -1) {
			tmp->wait_time += (time - tmp->start_wait);
			tmp->start_wait = -1;
		}
		tmp->core_id = core_id;
		core_arr[core_id] = 1;
		return tmp->id;
	}

}


/**
  Returns the average waiting time of all jobs scheduled by your scheduler.

  Assumptions:
    - This function will only be called after all scheduling is complete (all jobs that have arrived will have finished and no new jobs will arrive).
  @return the average waiting time of all jobs scheduled.
 */
float scheduler_average_waiting_time()
{
	// Declare the variables.	
	int i;
	job_t *tmp;
	int totalWaitTime = 0;
	// Get the head of job list.
	tmp = job_ptr;

	// for-loop to get all the jobs' wait time.
	for(i = 0; i < totalJobs; i++) {
		totalWaitTime += tmp->wait_time;
		tmp = tmp->next;

	}
	
	// Calculate the average waiting time.
	float avg_wait = (float)totalWaitTime / (float)totalJobs;
	
	return avg_wait;
}


/**
  Returns the average turnaround time of all jobs scheduled by your scheduler.

  Assumptions:
    - This function will only be called after all scheduling is complete (all jobs that have arrived will have finished and no new jobs will arrive).
  @return the average turnaround time of all jobs scheduled.
 */
float scheduler_average_turnaround_time()
{
	// Declare the variables.
	int i;
	job_t *tmp;
	int totalTurnTime = 0;
	// Get the head of job list.
	tmp = job_ptr;
	
	// for-loop to get all the jobs' turnover time.
	for(i = 0; i < totalJobs; i++) {
		totalTurnTime += tmp->turnover_time;
		tmp = tmp->next;
	}
	
	// Calculate the average turnover time.
	float avg_turn = (float)totalTurnTime / (float)totalJobs;
	
	return avg_turn;
}


/**
  Returns the average response time of all jobs scheduled by your scheduler.

  Assumptions:
    - This function will only be called after all scheduling is complete (all jobs that have arrived will have finished and no new jobs will arrive).
  @return the average response time of all jobs scheduled.
 */
float scheduler_average_response_time()
{
	// Declare the variables.
	int i;
	job_t *tmp;
	int resp_arr[totalJobs];
	int totalRespTime = 0;
	// Get the head of job list.
	tmp = job_ptr;
	
	// for-loop to get all the jobs' response time.
	for(i = 0; i < totalJobs; i++) {
		resp_arr[i] = tmp->response_time;
		totalRespTime += tmp->response_time;
		tmp = tmp->next;
	}
	
	// Calculate the average response time.
	float avg_resp = (float)totalRespTime / (float)totalJobs;
	
	return avg_resp;
}


/**
  Free any memory associated with your scheduler.
 
  Assumptions:
    - This function will be the last function called in your library.
*/
void scheduler_clean_up()
{
	// Declare the variables.
	int i = 0;
	job_t *tmp;
	
	// Delete and clear the memory used for job list.
	// Set the pointer as NULL.
	while((job_ptr != NULL) && (i < (totalJobs - 1)) && (totalJobs > 1))
	{
		tmp = job_ptr;
		job_ptr = job_ptr->next;
		tmp->next = NULL;
		free(tmp);
		tmp = NULL;
		i++;
	}
	
	if((i == (totalJobs - 1)) || (totalJobs == 1))
	{
		job_ptr->next = NULL;
		free(job_ptr);
		job_ptr = NULL;
	}

	// Clear the memory used for core array.
	// Set the pointer as NULL.
	free(core_arr);
	core_arr = NULL;

	// Clear the memory used for queue.
	// Set the pointer as NULL.
	priqueue_destroy(head);
	free(head);
	head = NULL;
}


/**
  This function may print out any debugging information you choose. This
  function will be called by the simulator after every call the simulator
  makes to your scheduler.
  In our provided output, we have implemented this function to list the jobs in the order they are to be scheduled. Furthermore, we have also listed the current state of the job (either running on a given core or idle). For example, if we have a non-preemptive algorithm and job(id=4) has began running, job(id=2) arrives with a higher priority, and job(id=1) arrives with a lower priority, the output in our sample output will be:

    2(-1) 4(0) 1(-1)  
  
  This function is not required and will not be graded. You may leave it
  blank if you do not find it useful.
 */
void scheduler_show_queue()
{
	int i;
	for(i = 0; i < priqueue_size(head); i++) {
		printf("%d(%d) ", ((job_t*)priqueue_at(head,i))->id, ((job_t*)priqueue_at(head,i))->core_id);
	}
}
