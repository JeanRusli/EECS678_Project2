/** @file libpriqueue.c
 */

#include <stdlib.h>
#include <stdio.h>

#include "libpriqueue.h"

/**
  Initializes the priqueue_t data structure.
  
  Assumtions
    - You may assume this function will only be called once per instance of priqueue_t
    - You may assume this function will be the first function called using an instance of priqueue_t.
  @param q a pointer to an instance of the priqueue_t data structure
  @param comparer a function pointer that compares two elements.
  See also @ref comparer-page
 */
void priqueue_init(priqueue_t *q, int(*comparer)(const void *, const void *))
{
	q->m_size = 0;
	q->m_comparer = comparer;
	q->m_data  = NULL;
}


/**
  Inserts the specified element into this priority queue.

  @param q a pointer to an instance of the priqueue_t data structure
  @param ptr a pointer to the data to be inserted into the priority queue
  @return The zero-based index where ptr is stored in the priority queue, where 0 indicates that ptr was stored at the front of the priority queue.
 */
int priqueue_offer(priqueue_t *q, void *ptr)
{
	q->m_size++;
	q->m_data = (void**)realloc(q->m_data, q->m_size * sizeof(void*));
	int index = 0;
	while((index<q->m_size-1) && (q->m_comparer(ptr,q->m_data[index]) > 0)){
		index++;
	}
	int i;
	for(i=q->m_size-1;i>index;i--){
		q->m_data[i] = q->m_data[i-1];
	}
	q->m_data[index] = ptr;

	return index;
	return -1;
}


/**
  Retrieves, but does not remove, the head of this queue, returning NULL if
  this queue is empty.
 
  @param q a pointer to an instance of the priqueue_t data structure
  @return pointer to element at the head of the queue
  @return NULL if the queue is empty
 */
void *priqueue_peek(priqueue_t *q)
{
	if(q->m_size > 0){
		return q->m_data[0];
	}
	return NULL;
}


/**
  Retrieves and removes the head of this queue, or NULL if this queue
  is empty.
 
  @param q a pointer to an instance of the priqueue_t data structure
  @return the head of this queue
  @return NULL if this queue is empty
 */
void *priqueue_poll(priqueue_t *q)
{
	if(q->m_size > 0){
		void* val = q->m_data[0];
		q->m_size--;
		for(int i = 0;i<q->m_size;i++){
			q->m_data[i] = q->m_data[i+1];
		}
		q->m_data = (void**)realloc(q->m_data, q->m_size * sizeof(void*));
		return val;
	}
	return NULL;
}


/**
  Returns the element at the specified position in this list, or NULL if
  the queue does not contain an index'th element.
 
  @param q a pointer to an instance of the priqueue_t data structure
  @param index position of retrieved element
  @return the index'th element in the queue
  @return NULL if the queue does not contain the index'th element
 */
void *priqueue_at(priqueue_t *q, int index)
{
	if((index>=0) && (index < q->m_size)){
		return q->m_data[index];
	}
	return NULL;
}


/**
  Removes all instances of ptr from the queue. 
  
  This function should not use the comparer function, but check if the data contained in each element of the queue is equal (==) to ptr.
 
  @param q a pointer to an instance of the priqueue_t data structure
  @param ptr address of element to be removed
  @return the number of entries removed
 */
int priqueue_remove(priqueue_t *q, void *ptr)
{
	int total = 0;
	for(int i = 0; i<q->m_size;i++){
		if(q->m_data[i] == ptr){
			q->m_size--;
			for(int j = i;j<q->m_size;j++){
				q->m_data[j] = q->m_data[j+1];
			}
			q->m_data = (void**)realloc(q->m_data, q->m_size * sizeof(void*));
			i--;
			total++;
		}
	}
	return total;
}


/**
  Removes the specified index from the queue, moving later elements up
  a spot in the queue to fill the gap.
 
  @param q a pointer to an instance of the priqueue_t data structure
  @param index position of element to be removed
  @return the element removed from the queue
  @return NULL if the specified index does not exist
 */
void *priqueue_remove_at(priqueue_t *q, int index)
{
	if((index>=0) && (index < q->m_size)){
		void* val = q->m_data[index];
		q->m_size--;
		for(int i = index;i<q->m_size;i++){
			q->m_data[i] = q->m_data[i+1];
		}
		q->m_data = (void**)realloc(q->m_data, q->m_size * sizeof(void*));
		return val;
	}
	return NULL;
}


/**
  Returns the number of elements in the queue.
 
  @param q a pointer to an instance of the priqueue_t data structure
  @return the number of elements in the queue
 */
int priqueue_size(priqueue_t *q)
{
	return q->m_size;
//	return 0;
}


/**
  Destroys and frees all the memory associated with q.
  
  @param q a pointer to an instance of the priqueue_t data structure
 */
void priqueue_destroy(priqueue_t *q)
{
	free(q->m_data);
	q->m_data = NULL;
	q->m_size = 0;
}
