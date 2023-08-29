/*

When we grab a job off of the realm jobs db, it gets marked as active. When
we set it to active, also set a session id to a unique UUID from that instance
of calling queue.start().

In getConcurrentJobs(), we should only grab jobs that do are active = false and
sessionId = null. This will prevent us from grabbing jobs that are already
active in another queue instance.


Send status change to server:
send breadcrumb trail of all status changes to server.


*/
try {
  if (await job.dependenciesMet(job)) {
    await this.worker.executeJob(job);
  } else {
    this.realm.write(() => {
      this.realm.write((job.active = false));
    });
    return;
  }
} catch (error) {
  this.realm.write(() => {
    this.realm.write((job.active = false));
  });
}
