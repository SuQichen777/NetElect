package grpcnet

import (
	"context"

	"raft-leader-election/server"

	"google.golang.org/grpc"
)

const (
	serviceName                 = "raft.Raft"
	requestVoteFullMethodName   = "/" + serviceName + "/RequestVote"
	appendEntriesFullMethodName = "/" + serviceName + "/AppendEntries"
	pingFullMethodName          = "/" + serviceName + "/Ping"
	clientReadFullMethodName    = "/" + serviceName + "/ClientRead"
	clientWriteFullMethodName   = "/" + serviceName + "/ClientWrite"
	penaltyScoreFullMethodName  = "/" + serviceName + "/PenaltyScore"
	petitionFullMethodName      = "/" + serviceName + "/Petition"
)

type RaftServiceServer interface {
	RequestVote(context.Context, *server.RequestVoteArgs) (*server.RequestVoteReply, error)
	AppendEntries(context.Context, *server.AppendEntriesArgs) (*server.AppendEntriesReply, error)
	Ping(context.Context, *server.PingArgs) (*server.PingReply, error)
	ClientRead(context.Context, *server.ClientReadArgs) (*server.ClientReadReply, error)
	ClientWrite(context.Context, *server.ClientWriteArgs) (*server.ClientWriteReply, error)
	PenaltyScore(context.Context, *server.PenaltyScoreArgs) (*server.PenaltyScoreReply, error)
	Petition(context.Context, *server.PetitionArgs) (*server.PetitionReply, error)
}

func RegisterRaftServiceServer(s grpc.ServiceRegistrar, srv RaftServiceServer) {
	s.RegisterService(&raftServiceDesc, srv)
}

type RaftServiceClient interface {
	RequestVote(ctx context.Context, in *server.RequestVoteArgs, opts ...grpc.CallOption) (*server.RequestVoteReply, error)
	AppendEntries(ctx context.Context, in *server.AppendEntriesArgs, opts ...grpc.CallOption) (*server.AppendEntriesReply, error)
	Ping(ctx context.Context, in *server.PingArgs, opts ...grpc.CallOption) (*server.PingReply, error)
	ClientRead(ctx context.Context, in *server.ClientReadArgs, opts ...grpc.CallOption) (*server.ClientReadReply, error)
	ClientWrite(ctx context.Context, in *server.ClientWriteArgs, opts ...grpc.CallOption) (*server.ClientWriteReply, error)
	PenaltyScore(ctx context.Context, in *server.PenaltyScoreArgs, opts ...grpc.CallOption) (*server.PenaltyScoreReply, error)
	Petition(ctx context.Context, in *server.PetitionArgs, opts ...grpc.CallOption) (*server.PetitionReply, error)
}

type raftServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewRaftServiceClient(cc grpc.ClientConnInterface) RaftServiceClient {
	return &raftServiceClient{cc: cc}
}

func (c *raftServiceClient) RequestVote(ctx context.Context, in *server.RequestVoteArgs, opts ...grpc.CallOption) (*server.RequestVoteReply, error) {
	out := new(server.RequestVoteReply)
	err := c.cc.Invoke(ctx, requestVoteFullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *raftServiceClient) AppendEntries(ctx context.Context, in *server.AppendEntriesArgs, opts ...grpc.CallOption) (*server.AppendEntriesReply, error) {
	out := new(server.AppendEntriesReply)
	err := c.cc.Invoke(ctx, appendEntriesFullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *raftServiceClient) Ping(ctx context.Context, in *server.PingArgs, opts ...grpc.CallOption) (*server.PingReply, error) {
	out := new(server.PingReply)
	err := c.cc.Invoke(ctx, pingFullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *raftServiceClient) ClientRead(ctx context.Context, in *server.ClientReadArgs, opts ...grpc.CallOption) (*server.ClientReadReply, error) {
	out := new(server.ClientReadReply)
	err := c.cc.Invoke(ctx, clientReadFullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *raftServiceClient) ClientWrite(ctx context.Context, in *server.ClientWriteArgs, opts ...grpc.CallOption) (*server.ClientWriteReply, error) {
	out := new(server.ClientWriteReply)
	err := c.cc.Invoke(ctx, clientWriteFullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *raftServiceClient) PenaltyScore(ctx context.Context, in *server.PenaltyScoreArgs, opts ...grpc.CallOption) (*server.PenaltyScoreReply, error) {
	out := new(server.PenaltyScoreReply)
	err := c.cc.Invoke(ctx, penaltyScoreFullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *raftServiceClient) Petition(ctx context.Context, in *server.PetitionArgs, opts ...grpc.CallOption) (*server.PetitionReply, error) {
	out := new(server.PetitionReply)
	err := c.cc.Invoke(ctx, petitionFullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func _RequestVote_Handler(srv interface{}, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	in := new(server.RequestVoteArgs)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(RaftServiceServer).RequestVote(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: requestVoteFullMethodName}
	handler := func(ctx context.Context, req any) (any, error) {
		return srv.(RaftServiceServer).RequestVote(ctx, req.(*server.RequestVoteArgs))
	}
	return interceptor(ctx, in, info, handler)
}

func _AppendEntries_Handler(srv interface{}, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	in := new(server.AppendEntriesArgs)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(RaftServiceServer).AppendEntries(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: appendEntriesFullMethodName}
	handler := func(ctx context.Context, req any) (any, error) {
		return srv.(RaftServiceServer).AppendEntries(ctx, req.(*server.AppendEntriesArgs))
	}
	return interceptor(ctx, in, info, handler)
}

func _Ping_Handler(srv interface{}, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	in := new(server.PingArgs)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(RaftServiceServer).Ping(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: pingFullMethodName}
	handler := func(ctx context.Context, req any) (any, error) {
		return srv.(RaftServiceServer).Ping(ctx, req.(*server.PingArgs))
	}
	return interceptor(ctx, in, info, handler)
}

func _ClientRead_Handler(srv interface{}, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	in := new(server.ClientReadArgs)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(RaftServiceServer).ClientRead(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: clientReadFullMethodName}
	handler := func(ctx context.Context, req any) (any, error) {
		return srv.(RaftServiceServer).ClientRead(ctx, req.(*server.ClientReadArgs))
	}
	return interceptor(ctx, in, info, handler)
}

func _ClientWrite_Handler(srv interface{}, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	in := new(server.ClientWriteArgs)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(RaftServiceServer).ClientWrite(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: clientWriteFullMethodName}
	handler := func(ctx context.Context, req any) (any, error) {
		return srv.(RaftServiceServer).ClientWrite(ctx, req.(*server.ClientWriteArgs))
	}
	return interceptor(ctx, in, info, handler)
}

func _PenaltyScore_Handler(srv interface{}, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	in := new(server.PenaltyScoreArgs)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(RaftServiceServer).PenaltyScore(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: penaltyScoreFullMethodName}
	handler := func(ctx context.Context, req any) (any, error) {
		return srv.(RaftServiceServer).PenaltyScore(ctx, req.(*server.PenaltyScoreArgs))
	}
	return interceptor(ctx, in, info, handler)
}

func _Petition_Handler(srv interface{}, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	in := new(server.PetitionArgs)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(RaftServiceServer).Petition(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: petitionFullMethodName}
	handler := func(ctx context.Context, req any) (any, error) {
		return srv.(RaftServiceServer).Petition(ctx, req.(*server.PetitionArgs))
	}
	return interceptor(ctx, in, info, handler)
}

var raftServiceDesc = grpc.ServiceDesc{
	ServiceName: serviceName,
	HandlerType: (*RaftServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{MethodName: "RequestVote", Handler: _RequestVote_Handler},
		{MethodName: "AppendEntries", Handler: _AppendEntries_Handler},
		{MethodName: "Ping", Handler: _Ping_Handler},
		{MethodName: "ClientRead", Handler: _ClientRead_Handler},
		{MethodName: "ClientWrite", Handler: _ClientWrite_Handler},
		{MethodName: "PenaltyScore", Handler: _PenaltyScore_Handler},
		{MethodName: "Petition", Handler: _Petition_Handler},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "raft",
}
