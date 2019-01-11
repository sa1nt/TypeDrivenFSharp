open System

type Timed<'a> = 
    {
       Started : DateTimeOffset
       Stopped : DateTimeOffset
       Result : 'a
    }
    member this.Duration = this.Stopped - this.Started

module Untimed = 
    let map f x = {
      Started = x.Started; Stopped = x.Stopped; Result = f x.Result 
    }

    let withResult newResult x = map (fun _ -> newResult) x

module Timed = 
    let capture clock x = 
        let now = clock()
        { Started = now; Stopped = now; Result = x }

    let map clock f x = 
        let result = f x.Result 
        let stopped = clock()
        { Started = x.Started; Stopped = stopped; Result = result }

    let timeOn clock f x = x |> capture clock |> map clock f

module Clocks = 
    let machineClock () = DateTimeOffset.Now

    let accClock (start : DateTimeOffset) rate () = 
        let now = DateTimeOffset.Now
        let elapsed = now - start
        start.AddTicks (elapsed.Ticks * rate) 

    open System.Collections.Generic
    
    let qClock (q : Queue<DateTimeOffset>) = q.Dequeue

    let seqClock (l : DateTimeOffset seq) = Queue<DateTimeOffset> l |> qClock

// Aux types
type MessageHandler = unit -> Timed<unit>

// State data
type ReadyData = Timed<TimeSpan list>

type ReceivedMessageData = Timed<TimeSpan list * MessageHandler>

type NoMessageData = Timed<TimeSpan list>

// States 
type PollingConsumer = 
    | ReadyState of ReadyData
    | ReceivedMessageState of ReceivedMessageData
    | NoMessageState of NoMessageData
    | StoppedState

// Transitions
let transitionFromStopped = StoppedState

let transitionFromNoMessage shouldIdle idle (nm : NoMessageData) = 
    if shouldIdle nm then 
        idle () |> Untimed.withResult nm.Result |> ReadyState
    else StoppedState

let transitionFromReady shouldPoll poll (r : ReadyData) = 
    if shouldPoll r then 
        let msg = poll ()
        match msg.Result with 
            | Some h -> msg |> Untimed.withResult (r.Result, h) |> ReceivedMessageState
            | None -> msg |> Untimed.withResult r.Result |> NoMessageState
    else StoppedState

let transitionFromReceived (rm : ReceivedMessageData) = 
    let durations, handleMessage = rm.Result
    let t = handleMessage ()
    let pollDuration = rm.Duration
    let handleDuration = t.Duration
    let totalDuration = pollDuration + handleDuration
    t |> Untimed.withResult (totalDuration :: durations) |> ReadyState

// State machine
let rec run trans state = 
    let nextState = trans state
    match nextState with 
        | StoppedState -> StoppedState
        | _ -> run trans nextState

let transition shouldPoll poll shouldIdle idle state = 
    match state with 
        | ReadyState r -> transitionFromReady shouldPoll poll r
        | ReceivedMessageState rm -> transitionFromReceived rm
        | NoMessageState nm -> transitionFromNoMessage shouldIdle idle nm
        | StoppedState -> transitionFromStopped

// Implementation functions 
let shouldIdle idleDuration stopBefore (nm : NoMessageData) : bool = 
    nm.Stopped + idleDuration < stopBefore

let idle (idleDuration : TimeSpan) () = 
    let s () = 
        idleDuration.TotalMilliseconds
        |> int
        |> Async.Sleep
        |> Async.RunSynchronously
    printfn "Sleeping for %d ms" (int idleDuration.TotalMilliseconds)
    Timed.timeOn Clocks.machineClock s ()

let fakeIdle duration () = 
    let now = DateTimeOffset.Now 
    {
        Started = now
        Stopped = now + duration
        Result = ()
    }

let shouldPoll calculateExpectedDuration stopBefore (r : ReadyData) = 
    let durations = r.Result
    let expectedHandleDuration = calculateExpectedDuration durations
    r.Stopped + expectedHandleDuration < stopBefore

let poll pollForMessage handle clock () : Timed<MessageHandler option> = 
    let p () = 
        match pollForMessage () with 
            | Some msg -> 
                let h () = Timed.timeOn clock (handle >> ignore) msg
                Some (h : MessageHandler)
            | None -> None
    Timed.timeOn clock p ()

let calculateAverageTimeSpan (durations : TimeSpan list) = 
    if durations.IsEmpty then None
    else 
        durations 
        |> List.averageBy (fun x -> float x.Ticks)
        |> int64
        |> TimeSpan.FromTicks
        |> Some

let calculateAverageAndStdDevTimeSpan durations : (TimeSpan * TimeSpan) option = 
    let stdDev (avg : TimeSpan) = 
        durations
        |> List.averageBy (fun x -> ((x - avg).Ticks |> float) ** 2.)
        |> sqrt
        |> int64
        |> TimeSpan.FromTicks
    durations |> calculateAverageTimeSpan |> Option.map (fun avg -> avg, stdDev avg)

let calculateExpectedDuration estimatedDuration durations : TimeSpan = 
    match calculateAverageAndStdDevTimeSpan durations with
        | None -> estimatedDuration
        | Some (avg, stdDev) -> avg + stdDev + stdDev + stdDev // avg + stdDev * 3

let simulatePollForMessage (r : Random) () = 
    let pollDuration = r.Next(100, 1000)
    printfn "Polling for %d ms" pollDuration
    pollDuration
    |> Async.Sleep
    |> Async.RunSynchronously

    let pollSuccessful = r.Next(0, 100) < 50
    if pollSuccessful then Some ()
    else None

let simlatedHandle (r : Random) () = 
    let handleDuration = r.Next(100, 1000)
    printfn "Handling for %d ms" handleDuration
    handleDuration
    |> Async.Sleep
    |> Async.RunSynchronously

// Configuration
let now' = DateTimeOffset.Now
let stopBefore' = now' + TimeSpan.FromSeconds 20.
let estimatedDuration' = TimeSpan.FromSeconds 2.
let idleDuration' = TimeSpan.FromSeconds 5.

// Compose functions
let shouldPoll' = shouldPoll (calculateExpectedDuration estimatedDuration') stopBefore'

let r' = Random ()
let handle' = simlatedHandle r'
let pollForMessage' = simulatePollForMessage r'
let poll' = poll pollForMessage' handle' Clocks.machineClock

let shouldIdle' = shouldIdle idleDuration' stopBefore'

let idle' = idle idleDuration'

let transition' = transition shouldPoll' poll' shouldIdle' idle'
let run' = run transition'

let result' = run'(ReadyState([] |> Timed.capture Clocks.machineClock))
