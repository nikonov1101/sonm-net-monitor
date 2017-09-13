package commands

import (
	"time"

	pb "github.com/sonm-io/core/proto"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

type CliInteractor interface {
	HubPing(context.Context) (*pb.PingReply, error)
	HubStatus(context.Context) (*pb.HubStatusReply, error)

	MinerList(context.Context) (*pb.ListReply, error)
	MinerStatus(minerID string, appCtx context.Context) (*pb.InfoReply, error)

	TaskList(appCtx context.Context, minerID string) (*pb.StatusMapReply, error)
	TaskLogs(appCtx context.Context, req *pb.TaskLogsRequest) (pb.Hub_TaskLogsClient, error)
	TaskStart(appCtx context.Context, req *pb.HubStartTaskRequest) (*pb.HubStartTaskReply, error)
	TaskStatus(appCtx context.Context, taskID string) (*pb.TaskStatusReply, error)
	TaskStop(appCtx context.Context, taskID string) (*pb.StopTaskReply, error)
}

type grpcInteractor struct {
	timeout time.Duration
	addr    string
}

func (it *grpcInteractor) call(addr string) (*grpc.ClientConn, error) {
	cc, err := grpc.Dial(addr, grpc.WithInsecure(),
		grpc.WithCompressor(grpc.NewGZIPCompressor()),
		grpc.WithDecompressor(grpc.NewGZIPDecompressor()))
	if err != nil {
		return nil, err
	}
	return cc, nil
}

func (it *grpcInteractor) ctx(appCtx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(appCtx, it.timeout)
}

func (it *grpcInteractor) HubPing(appCtx context.Context) (*pb.PingReply, error) {
	cc, err := it.call(it.addr)
	if err != nil {
		return nil, err
	}
	defer cc.Close()

	ctx, cancel := it.ctx(appCtx)
	defer cancel()

	return pb.NewHubClient(cc).Ping(ctx, &pb.PingRequest{})
}

func (it *grpcInteractor) HubStatus(appCtx context.Context) (*pb.HubStatusReply, error) {
	cc, err := it.call(it.addr)
	if err != nil {
		return nil, err
	}
	defer cc.Close()

	ctx, cancel := it.ctx(appCtx)
	defer cancel()

	return pb.NewHubClient(cc).Status(ctx, &pb.HubStatusRequest{})
}

func (it *grpcInteractor) MinerList(appCtx context.Context) (*pb.ListReply, error) {
	cc, err := it.call(it.addr)
	if err != nil {
		return nil, err
	}
	defer cc.Close()

	ctx, cancel := it.ctx(appCtx)
	defer cancel()

	return pb.NewHubClient(cc).List(ctx, &pb.ListRequest{})
}

func (it *grpcInteractor) MinerStatus(minerID string, appCtx context.Context) (*pb.InfoReply, error) {
	cc, err := it.call(it.addr)
	if err != nil {
		return nil, err
	}
	defer cc.Close()

	ctx, cancel := it.ctx(appCtx)
	defer cancel()

	var req = pb.HubInfoRequest{Miner: minerID}
	return pb.NewHubClient(cc).Info(ctx, &req)
}

func (it *grpcInteractor) TaskList(appCtx context.Context, minerID string) (*pb.StatusMapReply, error) {
	cc, err := it.call(it.addr)
	if err != nil {
		return nil, err
	}
	defer cc.Close()

	ctx, cancel := it.ctx(appCtx)
	defer cancel()

	req := &pb.HubStatusMapRequest{Miner: minerID}
	return pb.NewHubClient(cc).MinerStatus(ctx, req)
}

func (it *grpcInteractor) TaskLogs(appCtx context.Context, req *pb.TaskLogsRequest) (pb.Hub_TaskLogsClient, error) {
	cc, err := it.call(it.addr)
	if err != nil {
		return nil, err
	}
	defer cc.Close()
	return pb.NewHubClient(cc).TaskLogs(appCtx, req)
}

func (it *grpcInteractor) TaskStart(appCtx context.Context, req *pb.HubStartTaskRequest) (*pb.HubStartTaskReply, error) {
	cc, err := it.call(it.addr)
	if err != nil {
		return nil, err
	}
	defer cc.Close()

	ctx, cancel := it.ctx(appCtx)
	defer cancel()
	return pb.NewHubClient(cc).StartTask(ctx, req)
}

func (it *grpcInteractor) TaskStatus(appCtx context.Context, taskID string) (*pb.TaskStatusReply, error) {
	cc, err := it.call(it.addr)
	if err != nil {
		return nil, err
	}
	defer cc.Close()

	ctx, cancel := it.ctx(appCtx)
	defer cancel()

	var req = &pb.TaskStatusRequest{Id: taskID}
	return pb.NewHubClient(cc).TaskStatus(ctx, req)
}

func (it *grpcInteractor) TaskStop(appCtx context.Context, taskID string) (*pb.StopTaskReply, error) {
	cc, err := it.call(it.addr)
	if err != nil {
		return nil, err
	}
	defer cc.Close()

	ctx, cancel := it.ctx(appCtx)
	defer cancel()

	var req = &pb.StopTaskRequest{Id: taskID}
	return pb.NewHubClient(cc).StopTask(ctx, req)
}

func NewGrpcInteractor(addr string, to time.Duration) (CliInteractor, error) {
	i := &grpcInteractor{
		addr:    addr,
		timeout: to,
	}
	//err := i.call(addr)
	//if err != nil {
	//	return nil, err
	//}

	return i, nil
}
