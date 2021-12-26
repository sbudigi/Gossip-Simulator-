
#r "nuget: Akka"
#r "nuget: Akka.FSharp"
#r "nuget: Akka.TestKit"
#r "nuget: Akka.Remote"

open System
open System.Threading
open Akka.Actor
open Akka.Configuration
open Akka.FSharp
open Akka.TestKit

let rnd = System.Random(1)
let system = ActorSystem.Create("Gossip-Protocol")
// Input no of nodes in participation
let mutable nodeCount = fsi.CommandLineArgs.[1] |> int
// Type of topology - full , 3D , Imperfect 3D , line
let topology = fsi.CommandLineArgs.[2] |> string
// Type of algorithm - gossip , push-sum
let algorithm = fsi.CommandLineArgs.[3] |> string

// If the nodes have listened to the rumour gossipreceivelimit
let rumourreceivelimit = 10
// If the rumours have been sent to across more than gossipsendlimit nodes.
let rumoursendlimit = 1000

let timer = System.Diagnostics.Stopwatch()

// Initialisation
type DispatcherMsg =
    | InitTopology of string
    | TopologyBuildDone
    | NodeExhausted
    | PrintPsumResults

type TopologyMsg =
    | BuildTopology of string*list<IActorRef>
    | BuildDone

type NodeMsg =
    | AddTopology of string*IActorRef*list<IActorRef>
    | Rumor
    | Spread
    | Exhausted

type PNodeMsg =
    | AddTopologyP of string*IActorRef*list<IActorRef>
    | Send
    | Receive of float*float
    | Forward of float*float
    | ExhaustedP
    | PrintRatio


//Building the topology for gossip and push-sum algorithms
let Topology(mailbox:Actor<_>)=
    let mutable recordDone=0
    let mutable dispatcherref = null
    let rec loop() =actor{
        let! message = mailbox.Receive()
        match message with
        | BuildTopology(topology,nodelist)->    let mutable nlist=[]
                                                dispatcherref<-mailbox.Sender()
                                                if(topology="line") then
                                                    for i in 0 .. nodeCount-1 do
                                                        nlist <- []
                                                        if i <> 0 then //<> return true if the left side i is not equal to the right side
                                                            nlist <- nlist @ [nodelist.Item(i-1)] // @ concatenates two lists
                                                        if i <> nodeCount-1 then
                                                            nlist <- nlist @ [nodelist.Item(i+1)]

                                                        if algorithm = "gossip" then
                                                            nodelist.Item(i)<!AddTopology(topology,dispatcherref,nlist)
                                                        else
                                                            nodelist.Item(i)<!AddTopologyP(topology,dispatcherref,nlist)
                                                elif topology="full" then
                                                    for i in 0 .. nodeCount-1 do
                                                        if algorithm = "gossip" then
                                                            nodelist.Item(i)<!AddTopology(topology,dispatcherref,[])
                                                        else
                                                            nodelist.Item(i)<!AddTopologyP(topology,dispatcherref,[])
                                                elif topology="3D" || topology="imp3D" then
                                                    // let n= sqrt(nodecount|>float) |> int
                                                    let toFloat = nodeCount |> float
                                                    //let k =  (toFloat ** 0.33) |> ceil |> int
                                                    let k =  (toFloat ** 0.33) |> int
                                                    nodeCount<-k*k*k
                                                    for i in 0 .. nodeCount-1 do
                                                        nlist <- []
                                                        let level = i/(k*k)
                                                        // printfn "Number level level: %A" level
                                                        let upperlimit = (level+1)*k*k
                                                        let lowerlimit = level*k*k
                                                        if i-k >= lowerlimit then 
                                                            nlist <- nlist @ [nodelist.Item(i-k)]
                                                        if i+k < upperlimit then 
                                                            nlist <- nlist @ [nodelist.Item(i+k)]
                                                        if ((((i-1) % k) <> k-1) && (i-1 >= 0)) then 
                                                            nlist <- nlist @ [nodelist.Item(i-1)]
                                                        if (((i+1) % k) <> 0) then 
                                                            nlist <- nlist @ [nodelist.Item(i+1)]
                                                        if ((i + (k*k)) < nodeCount) then 
                                                            nlist <- nlist @ [nodelist.Item(i+k*k)]
                                                        if ((i - (k*k)) >= 0) then 
                                                            nlist <- nlist @ [nodelist.Item(i-k*k)]
                                                        
                                                        if topology = "imp3D" then 
                                                            nlist <- nlist @ [nodelist.Item(rnd.Next()%nodeCount)]

                                                        if algorithm = "gossip" then
                                                            nodelist.Item(i)<!AddTopology(topology,dispatcherref,nlist)  
                                                        else
                                                            nodelist.Item(i)<!AddTopologyP(topology,dispatcherref,nlist)                                             
        | BuildDone ->  
                        if  recordDone = nodeCount-1 then
                            dispatcherref<!TopologyBuildDone
                        recordDone<-recordDone+1
                        
        return! loop()
    }
    loop()
let topologyref= spawn system "topology" Topology
let mutable Nodelist = []
let Node(mailbox:Actor<_>)=
    let mutable neigbhours=[]
    let mutable rumorheardtimes = 0
    let mutable hadrumor = false
    let mutable spreadcnt = 0
    let mutable nodetopology=""
    let mutable exhausted = false
    let mutable nexhausted = 0
    let mutable dispatcherref = null
    let id = mailbox.Self.Path.Name |> int // ???
    let rec loop() =actor{
        let! message = mailbox.Receive()
        match message with
        | AddTopology(topology,dref,nodelist)->
                                                neigbhours<-nodelist
                                                nodetopology<-topology
                                                dispatcherref<-dref
                                                mailbox.Sender()<!BuildDone
        | Rumor ->  
                    if not exhausted then
                        if rumorheardtimes = 0 then
                            hadrumor <- true
                        rumorheardtimes<-rumorheardtimes+1
                        if rumorheardtimes = rumourreceivelimit then 
                            exhausted <- true
                            dispatcherref<!NodeExhausted
                            if topology = "full" then
                                for i in 0 .. nodeCount-1 do 
                                    if i <> id then
                                        Nodelist.Item(i)<!Exhausted
                            else
                                for i in 0 .. neigbhours.Length-1 do
                                    neigbhours.Item(i)<!Exhausted
                        else
                            mailbox.Self<!Spread
                        
        | Spread -> //printfn "Spread %i exhausted %b" id exhausted
                    if not exhausted then
                        let mutable next=rnd.Next()
                        if topology = "full" then
                            while next%nodeCount=id do
                                    next<-rnd.Next()
                            Nodelist.Item(next%nodeCount)<!Rumor
                        else
                            neigbhours.Item(next%neigbhours.Length)<!Rumor
                        spreadcnt <- spreadcnt + 1
                        if spreadcnt = rumoursendlimit then
                            exhausted <- true
                            dispatcherref<!NodeExhausted
                            if topology = "full" then
                                for i in 0 .. nodeCount-1 do 
                                    if i <> id then
                                        Nodelist.Item(i)<!Exhausted
                            else
                                for i in 0 .. neigbhours.Length-1 do
                                    Nodelist.Item(i)<!Exhausted
                        else
                            mailbox.Self<!Spread

        | Exhausted ->  if not hadrumor then
                            mailbox.Self<!Rumor
                        if not exhausted then
                            nexhausted <- nexhausted + 1
                            if topology = "full" then 
                                if nexhausted = nodeCount-1 then 
                                    exhausted <- true
                                    dispatcherref<!NodeExhausted
                            else
                                if nexhausted = neigbhours.Length then
                                    exhausted <- true
                                    dispatcherref<!NodeExhausted
        return! loop()
    }
    loop()


let PsumNode(mailbox:Actor<_>)=
    let mutable neigbhours=[]
    let mutable nodetopology=""
    let mutable exhausted = false
    let mutable nexhausted = 0
    let mutable ecnt = 0 // ????
    let mutable dispatcherref = null
    let id = mailbox.Self.Path.Name |> int //???
    let mutable s = id|>float
    let mutable w = 1.0
    let rec loop() =actor{
        let! message = mailbox.Receive()
        //printfn "%A %i" message id
        match message with
        | AddTopologyP(topology,dref,nodelist)->
                                                neigbhours<-nodelist
                                                nodetopology<-topology
                                                dispatcherref<-dref
                                                mailbox.Sender()<!BuildDone
        | Receive(rs,rw) ->  
                            if not exhausted then
                                let ns = s+rs
                                let nw = w+rw
                                if abs((ns/nw)-(s/w))<0.0000000001 then
                                    ecnt <- ecnt + 1
                                else
                                    ecnt <- 0
                                if ecnt = 3 then 
                                    exhausted <- true
                                    dispatcherref<!NodeExhausted
                                    if topology = "full" then
                                        for i in 0 .. nodeCount-1 do 
                                            if i <> id then
                                                Nodelist.Item(i)<!ExhaustedP
                                    else
                                        for i in 0 .. neigbhours.Length-1 do
                                            neigbhours.Item(i)<!ExhaustedP
                                    mailbox.Self<!Forward(rs,rw)
                                else
                                    s <- ns
                                    w <- nw
                                    mailbox.Self<!Send
                            else
                                mailbox.Self<!Forward(rs,rw)
        
        | Forward(rs,rw) -> let mutable next=rnd.Next()
                            if topology = "full" then
                                while next%nodeCount=id do
                                        next<-rnd.Next()
                                Nodelist.Item(next%nodeCount)<!Receive(rs,rw)
                            else
                                neigbhours.Item(next%neigbhours.Length)<!Receive(rs,rw)
                        
        | Send ->
                    if not exhausted then
                        let mutable next=rnd.Next()
                        s <- s/2.0
                        w <- w/2.0
                        if topology = "full" then
                            while next%nodeCount=id do
                                    next<-rnd.Next()
                            Nodelist.Item(next%nodeCount)<!Receive(s,w)
                        else
                            neigbhours.Item(next%neigbhours.Length)<!Receive(s,w)

        | ExhaustedP -> 
                        if not exhausted then
                            nexhausted <- nexhausted + 1
                            if topology = "full" then 
                                if nexhausted = nodeCount-1 then 
                                    exhausted <- true
                                    dispatcherref<!NodeExhausted
                            else
                                if nexhausted = neigbhours.Length then
                                    exhausted <- true
                                    dispatcherref<!NodeExhausted
        | PrintRatio -> printfn "Node: %i ratio: %f" id (s/w)
        return! loop()
    }
    loop()



if algorithm = "gossip" then
    Nodelist <- [for a in 0 .. nodeCount-1 do yield(spawn system (string a) Node)] 
else
    Nodelist <- [for a in 0 .. nodeCount-1 do yield(spawn system (string a) PsumNode)] 



let Dispatcher(mailbox:Actor<_>)=
    let mutable spread = 0  
    let rec loop() =actor{
        let! message = mailbox.Receive()
        //printfn "%A" message
        match message with
        | InitTopology(topology) ->     
                                        topologyref <! BuildTopology(topology,Nodelist)
        | TopologyBuildDone ->  
                                if algorithm = "gossip" then
                                    Nodelist.Item(rnd.Next()%nodeCount)<!Rumor
                                    timer.Start()

                                else
                                    let ind = rnd.Next()%nodeCount |> float
                                    Nodelist.Item(rnd.Next()%nodeCount)<!Receive(ind,1.0)
                                    timer.Start()

        | NodeExhausted ->  spread <- spread + 1
                            if spread = nodeCount then 
                                mailbox.Context.System.Terminate() |> ignore
                                printfn "Number of Nodes: %i" nodeCount
                                printfn "Topology       : %s" topology
                                printfn "Algorithm Used : %s" algorithm
                                printfn "time           : %i ms" timer.ElapsedMilliseconds
        | PrintPsumResults ->   for i in 0 .. nodeCount-1 do
                                    Nodelist.Item(i)<!PrintRatio
                                

                                
        return! loop()
    }
    loop()

let Dispatcherref = spawn system "Dispatcher" Dispatcher  

Dispatcherref<!InitTopology(topology)

system.WhenTerminated.Wait()