package main

import (
	"context"
	"github.com/ffhx1234/hflow-go/pipeline"
	"log"
	"os"
	"os/signal"
	"time"
)

type Packet struct {
	data string
}

type Frame struct {
	rgb string
	pts int64
	dts int64
}

type Rtsp struct {
	In  chan Packet `source:"in"`
	Out chan Packet `sink:"rtsp"`
}

type Gb28181 struct {
	In  chan Packet `source:"in"`
	Out chan Packet `sink:"gb28181"`
}

type Decoder struct {
	Rtsp chan Packet `source:"rtsp"`
	Gb   chan Packet `source:"gb28181"`
	Out  chan Frame  `sink:"rgb"`
}

type Player struct {
	In     chan Frame `source:"rgb"`
	Vlc    chan Frame `sink:"vlc"`
	Ffmpeg chan Frame `sink:"ffmpeg"`
}

type Vlc struct {
	In chan Frame `source:"vlc"`
}

type Ffmpeg struct {
	In chan Frame `source:"ffmpeg"`
}

func NewRtsp() Rtsp {
	r := Rtsp{
		In:  make(chan Packet),
		Out: make(chan Packet),
	}

	go func() {
		for i := 0; i < 10; i++ {
			r.In <- Packet{data: "rtsp"}
			time.Sleep(1 * time.Second)
		}
	}()
	return r
}

func (r Rtsp) Run(ctx context.Context) {
loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case it := <-r.In:
			r.Out <- Packet{data: it.data + " ++ Run"}
		}
	}
}

func NewGb28181() Gb28181 {
	g := Gb28181{
		In:  make(chan Packet),
		Out: make(chan Packet),
	}
	go func() {
		for i := 0; i < 10; i++ {
			g.In <- Packet{data: "gb28181"}
			time.Sleep(1 * time.Second)
		}
	}()
	return g
}

func (g Gb28181) Run(ctx context.Context) {
loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case it := <-g.In:
			g.Out <- Packet{data: it.data + "-- Run"}
		}
	}
}

func NewDecoder() Decoder {
	return Decoder{
		Rtsp: make(chan Packet),
		Gb:   make(chan Packet),
		Out:  make(chan Frame),
	}
}

func (d Decoder) Run(ctx context.Context) {
loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case item := <-d.Rtsp:
			select {
			case it := <-d.Gb:
				d.Out <- Frame{
					rgb: item.data + " " + it.data + " decoder ",
					pts: time.Now().Unix(),
					dts: time.Now().Unix(),
				}
			}
		}
	}
}

func NewPlayer() Player {
	return Player{
		In:     make(chan Frame),
		Vlc:    make(chan Frame),
		Ffmpeg: make(chan Frame),
	}
}

func (p Player) Run(ctx context.Context) {
loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case item := <-p.In:
			f := Frame{
				rgb: item.rgb + " ---player--- ",
				pts: item.pts,
				dts: item.dts,
			}
			p.Vlc <- f
			p.Ffmpeg <- f
		}
	}
}

func NewVlc() Vlc {
	return Vlc{In: make(chan Frame)}
}

func (v Vlc) Run(ctx context.Context) {
loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case it := <-v.In:
			log.Println("vlc: ", it.rgb, " ", it.pts, " ", it.dts)
		}
	}
}

func NewFfmpeg() Ffmpeg {
	return Ffmpeg{In: make(chan Frame)}
}

func (f Ffmpeg) Run(ctx context.Context) {
loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case it := <-f.In:
			log.Println("ffmpeg: ", it.rgb, " ", it.pts, " ", it.dts)
		}
	}
}

func main() {
	s := make(chan os.Signal, 1)
	signal.Notify(s, os.Interrupt)
	ctx, cancel := context.WithCancel(context.Background())
	ppl := pipeline.NewPipeline()
	ppl.Add("rtsp", "", NewRtsp())
	ppl.Add("gb28181", "", NewGb28181())
	decoder := NewDecoder()
	ppl.Add("rtsp", "decoder", decoder)
	ppl.Add("gb28181", "decoder", decoder)
	ppl.Add("decoder", "player", NewPlayer())
	ppl.Add("player", "vlc", NewVlc())
	ppl.Add("player", "ffmpeg", NewFfmpeg())
	ppl.Run(ctx)
Loop:
	for {
		select {
		case <-s:
			cancel()
			ppl.Close()
			break Loop
		}
	}
}
