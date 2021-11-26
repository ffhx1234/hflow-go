package pipeline

import (
	"context"
	"log"
	"reflect"
	"sync"
)

type Node interface {
	//Init()
	Run(ctx context.Context)
	//Close()
}

type NodeLink struct {
	name string
	node Node
}

type Pipeline struct {
	nodes     map[string]*NodeLink //map[node-name]NodeLink
	linkNodes map[string][]string  //map[current][]string{child1,child2...}
	lnMux     sync.Mutex
}

func NewPipeline() *Pipeline {
	return &Pipeline{
		nodes:     make(map[string]*NodeLink),
		linkNodes: make(map[string][]string),
	}
}

func (p *Pipeline) Add(parent, current string, node Node) *Pipeline {
	nodeLink := &NodeLink{
		name: current,
		node: node,
	}

	if parent == "" {
		//means this is a root node
		log.Panicln("luck of parent node!")
	} else if parent != "" && current != "" {
		p.lnMux.Lock()
		defer p.lnMux.Unlock()
		p.linkNodes[parent] = append(p.linkNodes[parent], current)
		p.nodes[current] = nodeLink
	} else {
		//means this is a root node
		p.nodes[parent] = nodeLink
	}
	return p
}

func (p *Pipeline) link(ctx context.Context, a, b Node) {
	var (
		aType, bType   reflect.Type
		aValue, bValue reflect.Value
		aOutV, bInV    reflect.Value
		linkTag        string
	)
	if a != nil {
		aType = reflect.TypeOf(a)
		aValue = reflect.ValueOf(a)
	}
	if b != nil {
		bType = reflect.TypeOf(b)
		bValue = reflect.ValueOf(b)
	}

	//option: a.out -> b.in
	for i := 0; i < aType.NumField(); i++ {
		if tag := aType.Field(i).Tag.Get("sink"); tag != "" {
			linkTag = tag
			aOutV = aValue.Field(i)
			break
		}
	}
	for j := 0; j < bType.NumField(); j++ {
		if tag := bType.Field(j).Tag.Get("source"); tag == linkTag {
			bInV = bValue.Field(j)
			break
		}
	}
	go func() {
	loop:
		for {
			select {
			case <-ctx.Done():
				break loop
			default:
				if item, ok := aOutV.Recv(); ok {
					bInV.Send(item)
				}
			}

		}
	}()

}

func (p *Pipeline) linkMulti(ctx context.Context, a Node, b []Node) {
	var (
		aType, bType   reflect.Type
		aValue, bValue reflect.Value
	)
	if a != nil {
		aType = reflect.TypeOf(a)
		aValue = reflect.ValueOf(a)
	}

	for _, node := range b {
		var (
			aOutV, bInV reflect.Value
			linkTag     string
		)
		if node != nil {
			bType = reflect.TypeOf(node)
			bValue = reflect.ValueOf(node)
		}

		//node b in
		for j := 0; j < bType.NumField(); j++ {
			if tag := bType.Field(j).Tag.Get("source"); tag != "" {
				bInV = bValue.Field(j)
				linkTag = tag
				break
			}
		}

		// node a out
		for i := 0; i < aType.NumField(); i++ {
			if tag := aType.Field(i).Tag.Get("sink"); tag == linkTag {
				aOutV = aValue.Field(i)
				break
			}
		}
		go func() {
		loop:
			for {
				select {
				case <-ctx.Done():
					break loop
				default:
					if item, ok := aOutV.Recv(); ok {
						bInV.Send(item)
					}
				}

			}
		}()
	}
}

func (p *Pipeline) Run(ctx context.Context) {
	l := len(p.nodes)
	if l == 0 {
		return
	}
	if l == 1 {
		go func() {
			for _, item := range p.nodes {
				go func(it *NodeLink) {
					args := []reflect.Value{reflect.ValueOf(ctx)}
					reflect.ValueOf(it.node).MethodByName("Run").Call(args)
				}(item)
			}
		}()
		return
	}
	p.lnMux.Lock()
	defer p.lnMux.Unlock()
	if len(p.linkNodes) > 0 {
		for k, v := range p.linkNodes {
			if len(v) == 1 {
				//means 1 to 1
				parentNode := p.nodes[k]
				currentNode := p.nodes[v[0]]
				p.link(ctx, parentNode.node, currentNode.node)
			} else if len(v) > 1 {
				//means 1 to n
				var currentNode []Node
				parentNode := p.nodes[k]
				for _, name := range v {
					currentNode = append(currentNode, p.nodes[name].node)
				}
				p.linkMulti(ctx, parentNode.node, currentNode)
			}
		}
	}

	//call all node Run function
	for _, item := range p.nodes {
		go func(it *NodeLink) {
			args := []reflect.Value{reflect.ValueOf(ctx)}
			reflect.ValueOf(it.node).MethodByName("Run").Call(args)
		}(item)
	}
}
