package smgr

import (
	"io"
	"time"

	"github.com/lucas-clemente/quic-go"
	"k8s.io/klog/v2"

	"github.com/kubeedge/kubeedge/pkg/viaduct/pkg/api"
)

// wrapper for session manager
type Stream struct {
	// the use type of stream only be stream or message
	UseType api.UseType
	// quic stream
	Stream quic.Stream
}

type Session struct {
	Sess quic.Session
}

func (s *Session) OpenStreamSync(streamUse api.UseType) (*Stream, error) {
	stream, err := s.Sess.OpenStreamSync()
	if err != nil {
		klog.Errorf("failed to open stream, error: %+v", err)
		return nil, err
	}

	_ = stream.SetWriteDeadline(time.Now().Add(10 * time.Second))
	defer stream.SetWriteDeadline(time.Time{})
	_, err = stream.Write([]byte(streamUse))
	if err != nil {
		klog.Errorf("write stream type, error: %+v", err)
		return nil, err
	}

	return &Stream{
		UseType: streamUse,
		Stream:  stream,
	}, nil
}

func (s *Session) AcceptStream() (*Stream, error) {
	stream, err := s.Sess.AcceptStream()
	if err != nil {
		klog.Errorf("failed to accept stream, error: %+v", err)
		return nil, err
	}

	_ = stream.SetReadDeadline(time.Now().Add(10 * time.Second))
	defer stream.SetReadDeadline(time.Time{})
	typeBytes := make([]byte, api.UseLen)
	_, err = io.ReadFull(stream, typeBytes)
	if err != nil {
		klog.Errorf("read stream type, error: %+v", err)
		return nil, err
	}

	klog.Infof("receive a stream(%s)", string(typeBytes))

	return &Stream{
		UseType: api.UseType(typeBytes),
		Stream:  stream,
	}, nil
}

func (s *Session) Close() error {
	return s.Sess.Close()
}
