package worker_pool

import (
	"database/sql"
	"fmt"
	"log"
	"sync"
)



type Job struct {
	Payload *sql.Stmt
	Args []interface{}
}

type Worker struct {
	WorkerPool chan chan *Job
	JobChannel chan  *Job
	quit chan bool
	Wg *sync.WaitGroup
}

func NewWorker(workerPool chan chan *Job, wg *sync.WaitGroup) Worker{
	return Worker{
		WorkerPool: workerPool,
		JobChannel: make(chan *Job, 1024 * 1024),
		quit: make(chan bool),
		Wg: wg,
	}
}

func (w Worker) Start() {
	go func() {
		for {
			w.WorkerPool <- w.JobChannel
			select {
			case job := <- w.JobChannel:
				{
				//w.Wg.Done()
				//if job.Args[0] != job.Args[0] || job.Args[1] != job.Args[1] {
				//	panic("End")
				//}

					//queryStart := time.Now()
					row := job.Payload.QueryRow(job.Args ...)
					//queryEnd := time.Now()
					var id int
					var firstName string
					var lastName string

					row.Scan(&id, &firstName, &lastName)
					//queryEnd.Sub(queryStart)
					if firstName != job.Args[0] || lastName != job.Args[1] {
						panic(fmt.Sprintf("Query for firstname: %s, lastname: %s; got firstname: %s, lastname: %s",
							job.Args[0], job.Args[1], firstName, lastName))
					}
					//log.Printf("Got this people with id: %d, firstname: %s, lastname: %s, took time: %v", id, firstName, lastName, queryEnd.Sub(queryStart))
					w.Wg.Done()
					//job.Payload.Close()


				}

			case <- w.quit:
				return
			}
		}
	}()
}

func (w Worker) Stop() {
	go func() {
		w.quit <- true
	}()
}

type Dispatcher struct {
	MaxWorkers int
	WorkerPool chan chan *Job
	JobQueue chan *Job
	Wg *sync.WaitGroup
}

func NewDispatcher(maxWorkers int, jobQueue chan *Job, waitGroup *sync.WaitGroup) *Dispatcher {
	pool := make(chan chan *Job, maxWorkers)
	return &Dispatcher{
		MaxWorkers: maxWorkers,
		WorkerPool: pool,
		JobQueue: jobQueue,
		Wg: waitGroup,
	}
}

func (d *Dispatcher) Run() {
	for i := 0; i < d.MaxWorkers ; i++ {
		worker := NewWorker(d.WorkerPool, d.Wg)
		worker.Start()
	}
	log.Printf("All %d workers are running, now you may dispatch jobs.", d.MaxWorkers)
	go d.dispatch()
}

func (d *Dispatcher) dispatch(){
	for {
		select {
		case job := <- d.JobQueue:
			go func(job *Job){
				jobChannel := <-d.WorkerPool
				jobChannel <- job
			}(job)
		}
	}
}