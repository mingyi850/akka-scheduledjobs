# Scheduled Jobs Framework

## Motivation
The purpose of this library is to simplify the creation of periodic jobs.

### Key Points

- Based on [akka-typed](https://doc.akka.io/docs/akka/current/typed/)
- v1 currently uses
   - [zookeeper](https://zookeeper.apache.org/) for local orchestration
   - consul for cross-dc coordination
- Pending migration to akka-cluster to support scalable job distribution and scheduling

### Supported Features
- Ready made traits to create recurring scheduled jobs
- Dynamic consul based configs for all scheduled jobs
- Job pacing, batching and retries
- Unified logging and metrics (TBD)
- Distributed scheduling/processing for jobs running in a cluster (TBD)



## Getting started

### Step 1: Create Processing Actor class
To see an example of how to create a job, you can refer to [here]() to show an completed example of a processor actor

The scheduledjobs framework is designed to be extensible. Users or contributors can create their own processor class to fit their own requirements, as long as it inherits the `ScheduledJobActor` abstract class.

The current implementation of the framework has one
```
abstract class BatchProcessActorTyped[Req, Res <: BatchResult[  
  Req  
], SettingsT <: BatchProcessActorSettings[SettingsT]]
    
def fetchAllBatches(): Future[List[Req]]
def processBatch(req: Req): Future[Res]
def batchRecover(req: Req): PartialFunction[Throwable, Res]
```
#### BatchProcessActor
The `BatchProcessActorTyped` class takes 3 type parameters - `Req`, `Res`, and `SettingsT`

These correspond to `Request`, `Response` and `SettingsType`.

#### Request
- A `Request` is a wrapper for some context to be processed by the job at a single point of time. 

- This could be a List of objects to be processed, or it can be an empty object if the process does not require any context. 
- We generate the list of requests to be processed by the job by implementing the `fetchAllBatches()` function.
- Each Request is processed by implementing
```processBatch(req: Req): Future[Res]```

#### Response
- A `Response` is a wrapper for some result of a process operation. It inherits `BatchResult`
``` 
abstract class BatchResult[Req](success: Boolean, req: Req) {  
  def isSuccess: Boolean = success  
  def request: Req = req  
}
```
- The provides information about the request and result of the operation so the framework can retry the process if required or drop it entirely.
- Implementing `batchRecover(req: Req): PartialFunction[Throwable, Res]`
lets the framework know how to handle Exceptions from processing and retry them.

#### SettingsType
The Settings type provides standard settings which are used by the framework. It should be extended with your own additional settings specific to the job, for example:
```
trait BatchProcessActorSettings[SettingsT] extends ScheduledJobSettings[SettingsT]
```
You can refer to the list of the required settings [here](BatchProcessingActorSettings)

####  Companion object for  Processor
```
trait ScheduledJob[BindingsT] {  
  def key: String  
  def create(bindings: BindingsT)(implicit  
  scheduledJobsContext: ScheduledJobsContext,  
  ec: ExecutionContext  
  ): ScheduledJobActor  
}
```
- The companion object for your processor should inherit the ScheduledJob trait which takes in a type `BindingsT` 
- `BindingsT` allows you to pass any container class which contains all required dependencies to create instances of your job actors.
- The `key` is the identifier which will be tied to the job throughout the framework
- `create` defines a way to create a new instance of the job from the bindings

### Step 2 - Define Settings Class
Settings specific to the actor can be defined by a case class. It should conform to the type constraints of the chosen processor.

#### Create a SettingsProvider
```
trait ScheduledJobsDynamicSettingsProvider[DummyBatchProcessingActorSettings]
```
- The companion object for your settings class should inherit `ScheduledJobsDynamicSettingsProvider` to create instances from the dynamic config via `getSettings`

### Step 3 - Create config
Please refer [here](reference.conf) for a full list of settings which can be configured

Some important ones
```
scheduledjobs {  
  settings {  
    application-name = "supply-cds-controller"  # used as part of context and metrics reporting
    consul-enabled =  true
    framework-log-level = "INFO"  # configure allowed log level from framework logs
    consul-watched-paths = ["global/it-be-bcom/my-app/", "local/app"] #consul paths to watch in order of increasing priority. Paths should have key scheduledjobs at their root 
  }  
  dummy-batch-processing {  # key here should match ScheduledJobs key
    batch-interval = 5 seconds  # delay between each process call
    retries = 3  # number of retries before dropping a request 
    schedule {  
      schedule-interval = 24 hours  #time between each job run
      anchor-time { #optional: sets the time that the first job will run. Use to ensure consistency in execution schedule
        hour = 7
        minute = 30
        second = 10
      }
      job-dedup-strategy = "drop" #defines what happens when new instance of a job is started while previous job is running. Allowed: ["run-concurrent", "drop", "enqueue"] 
    }  
  }  
}
```
### Step 4: Initialse ScheduledJobs
To start the scheduledjobs system, you can use the ScheduledJobsStarter class as part of application boot [here](scheduledJobsStartup)

```
import com.scheduledjobsframework.ScheduledJobsStarter

object Main extends App{
  ScheduledJobsStarter.init(
            "my-service-name",
            List(ScheduledJob1, ScheduledJob2, ScheduledJob3),
            SomeScheduledJobBindings()
          )
}

```
All Scheduledjobs should share the same Bindings class.
An example is provided [here](here)

## Reporting

TBD
## Implementation
Please refer to [confluence]() for more details about the implementation and design of the scheduledjobs framework



