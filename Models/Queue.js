/**
 *
 * Queue Model
 *
 * Queue Job Realm Schema defined in ../config/Database
 *
 */
import Database from '../config/Database';
import uuid from 'react-native-uuid';
import Worker from './Worker';
import promiseReflect from 'promise-reflect';

export class Queue {
  /**
   *
   * Set initial class properties.
   *
   * @constructor
   */
  constructor() {
    this.realm = null;
    this.worker = new Worker();
    this.status = 'inactive';
    this.statusChangeObserver = null;
  }

  /**
   *
   * Initializes the queue by connecting to Realm database.
   *
    Specify an optional options object to override the default realmPath.
   *
   */
  async init(options = {}) {
    if (this.realm === null) {
      this.realm = await Database.getRealmInstance(options);
    }
  }

  /**
   *
   * Add a worker function to the queue.
   *
   * Worker will be called to execute jobs associated with jobName.
   *
   * Worker function will receive job id and job payload as parameters.
   *
   * Example:
   *
   * function exampleJobWorker(id, payload) {
   *  console.log(id); // UUID of job.
   *  console.log(payload); // Payload of data related to job.
   * }
   *
   * @param jobName {string} - Name associated with jobs assigned to this worker.
   * @param worker {function} - The worker function that will execute jobs.
   * @param options {object} - Worker options. See README.md for worker options info.
   */
  addWorker(jobName, worker, options = {}) {
    this.worker.addWorker(jobName, worker, options);
  }

  /**
   *
   * Delete worker function from queue.
   *
   * @param jobName {string} - Name associated with jobs assigned to this worker.
   */
  removeWorker(jobName) {
    this.worker.removeWorker(jobName);
  }

/**
   * Get all of the registered workers.
   */
getWorkersAsArray() {
  return this.worker.getWorkersAsArray();
}

/**
 * Listen for changes in the queue status (starting and stopping). This method
 * returns a unsubscribe function to stop listening to events. Always ensure you
 * unsubscribe from the listener when no longer needed to prevent updates to
 * components no longer in use.
 *
 * #### Example
 *
 * ```js
 * const unsubscribe = queue.onQueueStateChanged((state) => {
 *   console.log(`Queue state changed to ${state}`);
 * });
 *
 * // Unsubscribe from further state changes
 * unsubscribe();
 * ```
 *
 * @param listener A listener function which triggers when queue status changed (for example starting).
 */
onQueueStateChanged(listener) {
  this.statusChangeObserver = listener;
  return () => {this.statusChangeObserver = null};
}

/**
 * Listen for changes in the jobs collection such as jobs changing status. This method
 * returns a unsubscribe function to stop listening to events. Always ensure you
 * unsubscribe from the listener when no longer needed to prevent updates to
 * components no longer in use.
 *
 * #### Example
 *
 * ```js
 * const unsubscribe = queue.onQueueJobChanged(() => {
 *  console.log(`A job changed!`);
 * });
 *
 * // Unsubscribe from further state changes
 * unsubscribe();
 * ```
 *
 * @param listener A listener function which triggers when jobs collection changed.
 */
onQueueJobChanged(listener) {
  // Add the listener callback to the realm
  try {
    this.realm.addListener("change", listener);
  } catch (error) {
    console.error(
      `An exception was thrown within the react native queue change listener: ${error}`
    );
  }

  return () => { this.realm.removeListener("change", listener); };
}

/**
 * A simple wrapper for setting the status of the queue and notifying any
 * listeners of the change.
 *
 * @private
 * @param {string} status
 */
changeStatus(status) {
  this.status = status;
  if (this.statusChangeObserver) {
    this.statusChangeObserver(status);
  }
}

  /**
   *
   * Creates a new job and adds it to queue.
   *
   * Queue will automatically start processing unless startQueue param is set to false.
   *
   * @param name {string} - Name associated with job. The worker function assigned to this name will be used to execute this job.
   * @param payload {object} - Object of arbitrary data to be passed into worker function when job executes.
   * @param options {object} - Job related options like timeout etc. See README.md for job options info.
   * @param startQueue - {boolean} - Whether or not to immediately begin prcessing queue. If false queue.start() must be manually called.
   */
  createJob(name, payload = {}, options = {}, startQueue = true) {
    if (!name) {
      throw new Error('Job name must be supplied.');
    }

    // Validate options
    if (options.timeout < 0 || options.attempts < 0) {
      throw new Error('Invalid job option.');
    }

    this.realm.write(() => {
      this.realm.create('Job', {
        id: uuid.v4(),
        name,
        payload: JSON.stringify(payload),
        data: JSON.stringify({
          attempts: options.attempts || 1
        }),
        priority: options.priority || 0,
        active: false,
        timeout: (options.timeout >= 0) ? options.timeout : 25000,
        created: new Date(),
        lastFailed: null,
        failed: null
      });
    });

    // Start queue on job creation if it isn't running by default.
    if (startQueue && this.status === 'inactive') {
      this.start();
    }
  }

  /**
   *
   * Start processing the queue.
   *
   * If queue was not started automatically during queue.createJob(), this
   * method should be used to manually start the queue.
   *
   * If queue.start() is called again when queue is already running,
   * queue.start() will return early with a false boolean value instead
   * of running multiple queue processing loops concurrently.
   *
   * Lifespan can be passed to start() in order to run the queue for a specific amount of time before stopping.
   * This is useful, as an example, for OS background tasks which typically are time limited.
   *
   * NOTE: If lifespan is set, only jobs with a timeout property at least 500ms less than remaining lifespan will be processed
   * during queue processing lifespan. This is to buffer for the small amount of time required to query Realm for suitable
   * jobs, and to mark such jobs as complete or failed when job finishes processing.
   *
   * IMPORTANT: Jobs with timeout set to 0 that run indefinitely will not be processed if the queue is running with a lifespan.
   *
   * @param lifespan {number} - If lifespan is passed, the queue will start up and run for lifespan ms, then queue will be stopped.
   * @return {boolean|undefined} - False if queue is already started. Otherwise nothing is returned when queue finishes processing.
   */
  async start(lifespan = 0) {
    // If queue is already running, don't fire up concurrent loop.
    if (this.status === 'active') {
      return false;
    }

    // this.status = 'active';
    this.changeStatus('active');

    // Get jobs to process
    const startTime = Date.now();
    let lifespanRemaining = null;
    let concurrentJobs = [];

    if (lifespan !== 0) {
      lifespanRemaining = lifespan - (Date.now() - startTime);
      lifespanRemaining = (lifespanRemaining === 0) ? -1 : lifespanRemaining; // Handle exactly zero lifespan remaining edge case.
      concurrentJobs = await this.getConcurrentJobs(lifespanRemaining);
    } else {
      concurrentJobs = await this.getConcurrentJobs();
    }

    while (this.status === 'active' && concurrentJobs.length) {
      // Loop over jobs and process them concurrently.
      const processingJobs = concurrentJobs.map( job => {
        return this.processJob(job);
      });

      // Promise Reflect ensures all processingJobs resolve so
      // we don't break await early if one of the jobs fails.
      await Promise.all(processingJobs.map(promiseReflect));

      // Get next batch of jobs.
      if (lifespan !== 0) {
        lifespanRemaining = lifespan - (Date.now() - startTime);
        lifespanRemaining = (lifespanRemaining === 0) ? -1 : lifespanRemaining; // Handle exactly zero lifespan remaining edge case.
        concurrentJobs = await this.getConcurrentJobs(lifespanRemaining);
      } else {
        concurrentJobs = await this.getConcurrentJobs();
      }
    }

    // this.status = 'inactive';
    this.changeStatus('inactive');
  }

  /**
   *
   * Stop processing queue.
   *
   * If queue.stop() is called, queue will stop processing until
   * queue is restarted by either queue.createJob() or queue.start().
   *
   */
  stop() {
    // this.status = 'inactive';
    this.changeStatus('inactive');
  }

  /**
   *
   * Get a job by id from the queue.
   *
   * @param sync {boolean} - This should be true if you want to guarantee job data is fresh. Otherwise you could receive job data that is not up to date if a write transaction is occuring concurrently.
   * @return {promise} - Promise that resolves to a collection of all the jobs in the queue.
   */
  async getJob(id, sync = false) {
    if (sync) {
      let job = null;
      this.realm.write(() => {
        job = this.realm.objectForPrimaryKey('Job', id);
      });

      return job;
    } else {
      return await this.realm.objectForPrimaryKey('Job', id);
    }
  }
  
  /**
   *
   * Get a collection of all the jobs in the queue.
   *
   * @param sync {boolean} - This should be true if you want to guarantee job data is fresh. Otherwise you could receive job data that is not up to date if a write transaction is occuring concurrently.
   * @return {promise} - Promise that resolves to a collection of all the jobs in the queue.
   */
  async getJobs(sync = false) {
    if (sync) {
      let jobs = null;
      this.realm.write(() => {
        jobs = Array.from(this.realm.objects('Job'));
      });

      return jobs;
    } else {
      return Array.from(await this.realm.objects('Job'));
    }
  }

  /**
   *
   * Get the next job(s) that should be processed by the queue.
   *
   * If the next job to be processed by the queue is associated with a
   * worker function that has concurrency X > 1, then X related (jobs with same name)
   * jobs will be returned.
   *
   *
   * @param queueLifespanRemaining {number} - The remaining lifespan of the current queue process (defaults to indefinite).
   * @return {promise} - Promise resolves to an array of job(s) to be processed next by the queue.
   */
  async getConcurrentJobs(queueLifespanRemaining = 0) {
    let concurrentJobs = [];

    const workersArr = this.worker.getWorkersAsArray();
    if (workersArr.length === 0) { return []; }

    this.realm.write(() => {
      // Get next job from queue.
      let nextJob = null;

      // Build query string
      // If queueLife
      const timeoutUpperBound = (queueLifespanRemaining - 500 > 0) ? queueLifespanRemaining - 499 : 0; // Only get jobs with timeout at least 500ms < queueLifespanRemaining.

      // Get worker specific minimum time between attempts.
      let workerFilters;
      if (workersArr.length > 0) {
        workerFilters = workersArr.map(worker => {
          const { name, minimumMillisBetweenAttempts = 0 } = worker;
          let earliestLastFailed = new Date();
          earliestLastFailed.setMilliseconds(earliestLastFailed.getMilliseconds() - minimumMillisBetweenAttempts);
          const realmFilterableDate = earliestLastFailed.toISOString().replace('T', '@').split('.')[0] + ':00';
          return `
          (
            name == "${name}" AND
            (
              lastFailed == null OR
              lastFailed <= ${realmFilterableDate}
            )
          )`;          
        });
      }

      const initialQuery = (queueLifespanRemaining)
      ? `
        (active == FALSE                AND
        failed == null                 AND
        (${workerFilters?.join(' OR ')}) AND
        timeout > 0                    AND
        timeout < ${timeoutUpperBound})
      OR (active == FALSE              AND
        failed == null                 AND
        (${workerFilters?.join(' OR ')}) AND
        timeout > 0                    AND
        timeout < ${timeoutUpperBound})`

      : `
        (active == FALSE                AND
        (${workerFilters?.join(' OR ')}) AND
        failed == null)
      OR (active == TRUE               AND
        (${workerFilters?.join(' OR ')}) AND
        failed == null)`;

      let jobs = Array.from(this.realm.objects('Job')
        .filtered(initialQuery)
        .sorted([['priority', true], ['created', false]]));

      if (jobs.length) {
        nextJob = jobs[0];
      }

      // If next job exists, get concurrent related jobs appropriately.
      if (nextJob) {
        const concurrency = this.worker.getConcurrency(nextJob.name);

        const allRelatedJobsQuery = (queueLifespanRemaining)
        ? `(name == "${nextJob.name}"   AND
            active == FALSE             AND
          (${workerFilters?.join(' OR ')}) AND
            failed == null              AND
            timeout > 0                 AND
            timeout < ${timeoutUpperBound})
          OR (name == "${nextJob.name}" AND
            active == FALSE             AND
          (${workerFilters?.join(' OR ')}) AND
            failed == null              AND
            timeout > 0                 AND
            timeout < ${timeoutUpperBound})`

        : `(name == "${nextJob.name}"   AND
            active == FALSE             AND
          (${workerFilters?.join(' OR ')}) AND
            failed == null)
          OR (name == "${nextJob.name}" AND
            active == TRUE              AND
          (${workerFilters?.join(' OR ')}) AND
            failed == null)`;

        const allRelatedJobs = this.realm.objects('Job')
          .filtered(allRelatedJobsQuery)
          .sorted([['priority', true], ['created', false]]);

        // Filter out any jobs that are not runnable.
        let runnableJobs = [];
        for (let index = 0; index < allRelatedJobs.length; index++) {
          const job = allRelatedJobs[index];
          const { runnable, reason } = this.worker.execIsJobRunnable(job.name, job);
          if (runnable) {
            runnableJobs.push(job);
          } else {
            const jobPayload = JSON.parse(job.payload);
            let jobData = JSON.parse(job.data);

            // Increment failed attempts number
            if (!jobData.skippedAttempts) {
              jobData.skippedAttempts = 1;
            } else {
              jobData.skippedAttempts++;
            }

            // Log skipped reasons
            if (!jobData.skippedReasons) {
              jobData.skippedReasons = [reason];
            } else {
              jobData.skippedReasons.push(reason);
            }

            job.data = JSON.stringify(jobData);

            // Fire onSkipped job lifecycle callback
            this.worker.executeJobLifecycleCallback('onSkipped', job.name, job.id, {...jobPayload, skippedReason: reason});
          }
        }

        let jobsToMarkActive = runnableJobs.slice(0, concurrency);

        // Grab concurrent job ids to reselect jobs as marking these jobs as active will remove
        // them from initial selection when write transaction exits.
        // See: https://stackoverflow.com/questions/47359368/does-realm-support-select-for-update-style-read-locking/47363356#comment81772710_47363356
        const concurrentJobIds = jobsToMarkActive.map(job => job.id);

        // Mark concurrent jobs as active
        jobsToMarkActive = jobsToMarkActive.map(job => {
          job.active = true;
        });

        // Reselect now-active concurrent jobs by id.
        if (concurrentJobIds.length > 0) {
          const reselectQuery = concurrentJobIds.map(jobId => 'id == "' + jobId + '"').join(' OR ');
          const reselectedJobs = Array.from(this.realm.objects('Job')
            .filtered(reselectQuery)
            .sorted([['priority', true], ['created', false]]));
          concurrentJobs = reselectedJobs.slice(0, concurrency);
        }
      }
    });

    return concurrentJobs;

  }

  /**
   *
   * Process a job.
   *
   * Job lifecycle callbacks are called as appropriate throughout the job processing lifecycle.
   *
   * Job is deleted upon successful completion.
   *
   * If job fails execution via timeout or other exception, error will be
   * logged to job.data.errors array and job will be reset to inactive status.
   * Job will be re-attempted up to the specified "attempts" setting (defaults to 1),
   * after which it will be marked as failed and not re-attempted further.
   *
   * @param job {object} - Job realm model object
   */
  async processJob(job) {
    // Data must be cloned off the realm job object for several lifecycle callbacks to work correctly.
    // This is because realm job is deleted before some callbacks are called if job processed successfully.
    // More info: https://github.com/billmalarky/react-native-queue/issues/2#issuecomment-361418965
    const jobName = job.name;
    const jobId = job.id;
    const jobPayload = JSON.parse(job.payload);

    // Fire onStart job lifecycle callback
    this.worker.executeJobLifecycleCallback('onStart', jobName, jobId, jobPayload);

    try {
      await this.worker.executeJob(job);

      // On successful job completion, remove job
      this.realm.write(() => {
        this.realm.delete(job);
      });

      // Job has processed successfully, fire onSuccess and onComplete job lifecycle callbacks.
      this.worker.executeJobLifecycleCallback('onSuccess', jobName, jobId, jobPayload);
      this.worker.executeJobLifecycleCallback('onComplete', jobName, jobId, jobPayload);
    } catch (error) {
      // Handle job failure logic, including retries.
      let jobData = JSON.parse(job.data);
      const errorMessage = error?.message || '';

      // Call the optional error profiler from the worker.options to learn what we should
      // do with this error. If the profiler returns true, we should attempt the job.
      const failureBehavior = this.worker.getFailureBehavior(jobName);

      switch (failureBehavior) {
        case 'standard':
          this.realm.write(() => {
            // Increment failed attempts number
            if (!jobData.failedAttempts) {
              jobData.failedAttempts = 1;
            } else {
              jobData.failedAttempts++;
            }

            // Log error
            if (!jobData.errors) {
              jobData.errors = [errorMessage];
            } else {
              jobData.errors.push(errorMessage);
            }

            job.data = JSON.stringify(jobData);

            // Reset active status
            job.active = false;

            // Use the same date object for both failure times if last failure
            const now = new Date();

            // Record when this attempt failed
            job.lastFailed = now;

            // Mark job as failed if too many attempts
            if (jobData.failedAttempts >= jobData.attempts) {
              job.failed = now;
            }
          });

          // Execute job onFailure lifecycle callback.
          this.worker.executeJobLifecycleCallback('onFailure', jobName, jobId, jobPayload);

          // If job has failed all attempts execute job onFailed and onComplete lifecycle callbacks.
          if (jobData.failedAttempts >= jobData.attempts) {
            this.worker.executeJobLifecycleCallback('onFailed', jobName, jobId, jobPayload);
            this.worker.executeJobLifecycleCallback('onComplete', jobName, jobId, jobPayload);
          }
          break;
        default:
          break;
      }
    }
  }

   /**
   * Delete a job from the queue.
   *
   * @param jobId {string} - Unique id associated with job.
   *
   */

   deleteJob(jobId) {
    this.realm.write(() => {
      let job = this.realm.objects('Job').filtered('id == "' + jobId + '"');

      if (job.length) {
        this.realm.delete(job);
      } else {
        throw new Error('Job ' + jobId + ' does not exist.');
      }
    });
  }

  /**
   *
   * Delete all failed jobs from the queue.
   *
   *
   */

  deleteAllFailedJobs() {
    this.realm.write(() => {
      let jobs = Array.from(this.realm.objects('Job')
        .filtered('failed != null'));

      if (jobs.length) {
        this.realm.delete(jobs);
      }
    });
  }

  /**
   *
   * Delete jobs in the queue.
   *
   * If jobName is supplied, only jobs associated with that name
   * will be deleted. Otherwise all jobs in queue will be deleted.
   *
   * @param jobName {string|null} - Name associated with job (and related job worker).
   */
  flushQueue(jobName = null) {
    if (jobName) {
      this.realm.write(() => {
        let jobs = Array.from(this.realm.objects('Job')
          .filtered('name == "' + jobName + '"'));

        if (jobs.length) {
          this.realm.delete(jobs);
        }
      });
    } else {
      this.realm.write(() => {
        this.realm.deleteAll();
      });
    }
  }
}

/**
 *
 * Factory should be used to create a new queue instance.
 *
 * @return {Queue} - A queue instance.
 */
export default async function queueFactory(options = {}) {
  const queue = new Queue();
  await queue.init(options);

  return queue;
}
