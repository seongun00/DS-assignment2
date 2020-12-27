package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"
import "sync/atomic"


type ViewServer struct {
	mu                   sync.Mutex
	l                    net.Listener
	dead                 int32 // for testing
	rpccount             int32 // for testing
	me                   string

	// Your declarations here.
	currView             View                  //  keeps track of current view
	pingTimeMap          map[string]time.Time  //  keeps track of most recent time VS heard ping from each server
	primaryAckedCurrView bool                  //  keeps track of whether primary has ACKed the current view
	idleServer           string                //  keeps track of any idle servers
	cvar				bool				//상태 확인 변수
	reset				bool				//
}


//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {
	vs.mu.Lock()
	defer vs.mu.Unlock()

	// Your code here.	

	// 1. Update ping times for current server
	vs.pingTimeMap[args.Me]=time.Now()
	// 2. Update view and/or idle server if reboot or new server, or ACK the current view
	if vs.currView.Primary==""&& vs.currView.Viewnum==args.Viewnum{
		vs.currView.Primary=args.Me
		vs.currView.Viewnum+=1
		reply.View=vs.currView
		
	}else{
		
		if args.Me==vs.currView.Primary{
			
			if args.Viewnum==0{
				
				if vs.reset==true{
					
					vs.currView.Viewnum+=1
					vs.reset = false
					vs.cvar = false
					reply.View=vs.currView
					
				}else{
					
					vs.currView.Viewnum+=1
					vs.currView.Primary = vs.currView.Backup
					vs.currView.Backup = vs.idleServer
					vs.idleServer = ""
					vs.cvar = true
					reply.View = vs.currView
					
				
				}
			}else{
				if args.Viewnum == vs.currView.Viewnum{
					vs.primaryAckedCurrView = true
				}
				
				if vs.reset == true{
					vs.currView.Viewnum+=1
					vs.reset = false
				}
				reply.View = vs.currView
				
				
				if vs.primaryAckedCurrView == true && vs.cvar == true{
					vs.cvar = false
				}
			}
		}else{
			
			if vs.currView.Backup==""{
				vs.currView.Backup=args.Me
				vs.cvar = true
				vs.currView.Viewnum+=1
				reply.View = vs.currView
				vs.primaryAckedCurrView = false
				
			}else{
				
				if args.Me == vs.currView.Backup{
					if args.Viewnum == 0 && vs.currView.Viewnum!=0&&vs.primaryAckedCurrView==true{
						if vs.idleServer!=""{
							vs.currView.Viewnum+=1
							k:=vs.currView.Backup
							vs.currView.Backup=vs.idleServer
							vs.idleServer= k
							vs.cvar = true
							
						}else{
							
							vs.cvar = true
						}
						
						reply.View= vs.currView
						
					}else{
						reply.View = vs.currView
					}
					
				}else{
					if vs.idleServer == ""{
						vs.idleServer=args.Me
						reply.View=vs.currView
						
						
					}else{
						reply.View=vs.currView
					}
				}
				
			}
		}
	}

	return nil
}


//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {
	vs.mu.Lock()
	defer vs.mu.Unlock()

	// Your code here.	
	// Add view to the reply message
	reply.View=vs.currView

	return nil
}


//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {
	vs.mu.Lock()
	defer vs.mu.Unlock()	

	// Your code here.	

	// 1. No recent pings from the idle server
	if vs.pingTimeMap[vs.idleServer].UnixNano()!=6795364578871345152{
		
		if vs.primaryAckedCurrView == true && time.Now().UnixNano()-vs.pingTimeMap[vs.idleServer].UnixNano()>DeadPings*int64(PingInterval){
		vs.idleServer=""	
		}
	}
	

	// 2. No recent pings from the backup
	if vs.pingTimeMap[vs.currView.Backup].UnixNano()!= 6795364578871345152{
		if vs.primaryAckedCurrView==true && time.Now().UnixNano()-vs.pingTimeMap[vs.currView.Backup].UnixNano()>DeadPings*int64(PingInterval){
			
			vs.currView.Backup=vs.idleServer
			vs.idleServer = ""
			
		}
	}
	

	// 3. No recent pings from the primary
	if vs.pingTimeMap[vs.currView.Primary].UnixNano()!=6795364578871345152{
		
		if vs.primaryAckedCurrView == true && time.Now().UnixNano()-vs.pingTimeMap[vs.currView.Primary].UnixNano()>DeadPings*int64(PingInterval){
			vs.currView.Primary = vs.currView.Backup
			vs.currView.Backup= vs.idleServer
			vs.idleServer=""
			
			vs.reset = true
			vs.cvar = true
					fmt.Println("primary dead")
		}
	}
	
}


//
// tell the server to shut itself down.
// for testing.
// please don't change these two functions.
//
func (vs *ViewServer) Kill() {
	atomic.StoreInt32(&vs.dead, 1)
	vs.l.Close()
}

//
// has this server been asked to shut down?
//
func (vs *ViewServer) isdead() bool {
	return atomic.LoadInt32(&vs.dead) != 0
}


// please don't change this function.
func (vs *ViewServer) GetRPCCount() int32 {
	return atomic.LoadInt32(&vs.rpccount)
}


func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	// Your vs.* initializations here.
	vs.currView = View{Viewnum: 0, Primary: "", Backup: ""}
	vs.pingTimeMap = make(map[string]time.Time)
	vs.primaryAckedCurrView = false
	vs.idleServer = ""

	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.isdead() == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.isdead() == false {
				atomic.AddInt32(&vs.rpccount, 1)
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.isdead() == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.isdead() == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}