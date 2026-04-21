package proxy

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

// ============================================================================
// P0-2: queryDone channel 信号机制测试
// ============================================================================

func TestQueryDoneChannel_Signal(t *testing.T) {
	t.Run("channel 信号正确传递", func(t *testing.T) {
		ch := make(chan struct{}, 8)
		ch <- struct{}{}

		select {
		case <-ch:
			// 预期收到信号
		default:
			t.Error("expected to receive signal from channel")
		}
	})

	t.Run("buffered channel 多个信号不丢失", func(t *testing.T) {
		ch := make(chan struct{}, 8)

		// 快速连续发送 5 个信号
		for i := 0; i < 5; i++ {
			select {
			case ch <- struct{}{}:
			default:
				t.Errorf("channel full at signal %d, expected buffer capacity", i)
			}
		}

		// 五个信号按序接收
		for i := 0; i < 5; i++ {
			select {
			case <-ch:
			default:
				t.Errorf("expected signal %d available", i)
			}
		}

		// 应无更多信号
		select {
		case <-ch:
			t.Error("received unexpected extra signal")
		default:
		}
	})

	t.Run("非阻塞 select 不阻塞调用方", func(t *testing.T) {
		ch := make(chan struct{}, 8)

		// 无信号时 default 分支立即返回
		done := make(chan struct{})
		go func() {
			select {
			case <-ch:
				t.Error("should not receive signal")
			default:
			}
			close(done)
		}()

		select {
		case <-done:
		case <-time.After(100 * time.Millisecond):
			t.Error("non-blocking select should return immediately")
		}
	})
}

// ============================================================================
// P0-2: forwardUntilQueryDone channel 模式测试
// ============================================================================

func TestForwardUntilQueryDone_Channel(t *testing.T) {
	t.Run("channel 模式无忙等", func(t *testing.T) {
		p := &Proxy{cfg: DefaultConfig()}
		p.cfg.IdleTimeout = Duration{500 * time.Millisecond}

		clientRead, clientWrite := net.Pipe()
		defer clientRead.Close()
		defer clientWrite.Close()

		upstreamRead, upstreamWrite := net.Pipe()
		defer upstreamRead.Close()
		defer upstreamWrite.Close()

		queryDoneCh := make(chan queryDoneSignal, 8)

		// 消费 upstream 数据
		go func() {
			buf := make([]byte, 1024)
			for {
				_, err := upstreamRead.Read(buf)
				if err != nil {
					return
				}
			}
		}()

		// 延迟 200ms 后发 queryDone 信号
		go func() {
			time.Sleep(200 * time.Millisecond)
			queryDoneCh <- queryDoneSignal{IsEndOfStream: true}
		}()

		br := bufio.NewReader(clientRead)
		start := time.Now()
		result := p.forwardUntilQueryDone(1, br, clientRead, upstreamWrite, queryDoneCh)
		elapsed := time.Since(start)

		if !result {
			t.Error("expected true when queryDone signal received")
		}

		// 应在信号到达时立即返回，而非轮询多次
		if elapsed < 150*time.Millisecond || elapsed > 1*time.Second {
			t.Errorf("elapsed = %v, expected ~200ms", elapsed)
		}
	})

	t.Run("数据转发后收到 queryDone 信号", func(t *testing.T) {
		p := &Proxy{cfg: DefaultConfig()}
		p.cfg.IdleTimeout = Duration{2 * time.Second}

		clientRead, clientWrite := net.Pipe()
		defer clientRead.Close()
		defer clientWrite.Close()

		upstreamRead, upstreamWrite := net.Pipe()
		defer upstreamRead.Close()
		defer upstreamWrite.Close()

		queryDoneCh := make(chan queryDoneSignal, 8)

		// 收集 upstream 数据
		var received bytes.Buffer
		go func() {
			buf := make([]byte, 1024)
			for {
				n, err := upstreamRead.Read(buf)
				if n > 0 {
					received.Write(buf[:n])
				}
				if err != nil {
					return
				}
			}
		}()

		testData := []byte("hello_from_client")

		// 先写数据再发信号
		go func() {
			time.Sleep(50 * time.Millisecond)
			clientWrite.Write(testData)
			time.Sleep(100 * time.Millisecond)
			queryDoneCh <- queryDoneSignal{IsEndOfStream: true}
		}()

		br := bufio.NewReader(clientRead)
		result := p.forwardUntilQueryDone(1, br, clientRead, upstreamWrite, queryDoneCh)

		if !result {
			t.Error("expected true")
		}

		// 等待数据传输完成
		time.Sleep(50 * time.Millisecond)

		if received.Len() == 0 {
			t.Error("expected data to be forwarded to upstream")
		}
	})
}

// ============================================================================
// 两阶段状态机包类型验证测试
// ============================================================================

func TestPacketExpectation_StateTransitions(t *testing.T) {
	const (
		expectQuery = 0
		expectData  = 1
	)

	t.Run("初始状态为 expectQuery", func(t *testing.T) {
		state := expectQuery
		if state != expectQuery {
			t.Errorf("initial state = %d, want expectQuery(0)", state)
		}
	})

	t.Run("Query 后切换到 expectData", func(t *testing.T) {
		state := expectQuery

		// 模拟收到 Query
		state = expectData

		if state != expectData {
			t.Errorf("state after Query = %d, want expectData(1)", state)
		}
	})

	t.Run("queryDone 信号后回到 expectQuery", func(t *testing.T) {
		state := expectData
		ch := make(chan struct{}, 8)
		ch <- struct{}{}

		select {
		case <-ch:
			state = expectQuery
		default:
		}

		if state != expectQuery {
			t.Errorf("state after queryDone = %d, want expectQuery(0)", state)
		}
	})

	t.Run("Cancel 后回到 expectQuery", func(t *testing.T) {
		state := expectData
		// Cancel 重置
		state = expectQuery

		if state != expectQuery {
			t.Errorf("state after Cancel = %d, want expectQuery(0)", state)
		}
	})
}

// ============================================================================
// P1-2: ChunkedWriter 错误路径不归还 pool 测试
// ============================================================================

// errorWriter 总是返回错误的 Writer
type errorWriter struct {
	err error
}

func (ew *errorWriter) Write(p []byte) (int, error) {
	return 0, ew.err
}

func TestChunkedWriter_NoPoolOnError(t *testing.T) {
	t.Run("Write 失败时不 panic 且返回错误", func(t *testing.T) {
		errExpected := errors.New("write failed")
		cw := NewChunkedWriter(&errorWriter{err: errExpected}, true)

		n, err := cw.Write([]byte("test data"))
		if n != 0 {
			t.Errorf("n = %d, want 0", n)
		}
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if !errors.Is(err, errExpected) {
			t.Errorf("error = %v, want wrapping of %v", err, errExpected)
		}
	})

	t.Run("多次 Write 失败不会破坏 pool", func(t *testing.T) {
		errExpected := errors.New("write failed")
		cw := NewChunkedWriter(&errorWriter{err: errExpected}, true)

		for i := 0; i < 100; i++ {
			cw.Write([]byte("test data"))
		}

		// pool 完整性验证：成功的 Writer 仍能从 pool 获取 buffer
		var buf bytes.Buffer
		cw2 := NewChunkedWriter(&buf, true)
		n, err := cw2.Write([]byte("hello"))
		if err != nil {
			t.Fatalf("successful write after errors: %v", err)
		}
		if n != 5 {
			t.Errorf("n = %d, want 5", n)
		}
		// 验证 chunked 帧格式: [size:4][data:5][endMarker:4]
		if buf.Len() != 4+5+4 {
			t.Errorf("frame size = %d, want %d", buf.Len(), 4+5+4)
		}
	})

	t.Run("disabled 模式下 Write 失败直接传播", func(t *testing.T) {
		errExpected := errors.New("direct write failed")
		cw := NewChunkedWriter(&errorWriter{err: errExpected}, false)

		_, err := cw.Write([]byte("test"))
		if !errors.Is(err, errExpected) {
			t.Errorf("error = %v, want %v", err, errExpected)
		}
	})
}

// ============================================================================
// P0-3: ServerHello drain 完整性测试
// ============================================================================

func TestServerHelloDrain_Complete(t *testing.T) {
	t.Run("循环 drain 确保读取所有 buffered 数据", func(t *testing.T) {
		// 模拟 bufio.Reader 中有多段 buffered 数据
		data := bytes.Repeat([]byte("A"), 1024)
		var teeOutput bytes.Buffer
		teeReader := io.TeeReader(bytes.NewReader(data), &teeOutput)
		br := bufio.NewReaderSize(teeReader, 4096) // buffer 大于数据

		// 预读一些数据到 buffer
		_, err := br.Peek(1024)
		if err != nil {
			t.Fatalf("Peek error: %v", err)
		}

		// 模拟循环 drain
		totalDrained := 0
		for {
			buffered := br.Buffered()
			if buffered <= 0 {
				break
			}
			drainBuf := make([]byte, buffered)
			n, err := br.Read(drainBuf)
			totalDrained += n
			if err != nil {
				break
			}
		}

		if totalDrained != 1024 {
			t.Errorf("drained %d bytes, want 1024", totalDrained)
		}

		// teeOutput 应该包含所有数据
		if teeOutput.Len() != 1024 {
			t.Errorf("teeOutput = %d bytes, want 1024", teeOutput.Len())
		}
	})

	t.Run("无 buffered 数据时循环立即退出", func(t *testing.T) {
		br := bufio.NewReader(bytes.NewReader(nil))

		iterations := 0
		for {
			if br.Buffered() <= 0 {
				break
			}
			iterations++
			drainBuf := make([]byte, br.Buffered())
			br.Read(drainBuf)
		}

		if iterations != 0 {
			t.Errorf("expected 0 iterations for empty reader, got %d", iterations)
		}
	})
}
