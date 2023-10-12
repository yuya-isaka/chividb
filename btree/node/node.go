package node

import (
	"fmt"

	"github.com/yuya-isaka/chibidb/disk"
	"github.com/yuya-isaka/chibidb/pool"
)

// ======================================================================

type NodeType interface {
	nodetypeProtexted()
}

type nt string

func (n nt) nodetypeProtexted() {}

const (
	MetaNodeType   nt = "META    " // メタノード、8 bytes
	LeafNodeType   nt = "LEAF    " // 葉ノード、8 bytes
	BranchNodeType nt = "BRANCH  " // 枝ノード、8 bytes
)

// ======================================================================

type Node struct {
	nodeUpdate *bool  // 1 byte
	nodeType   []byte // 8 bytes, MetaNodeTyoe or LeafNodeType or BranchNodeType
	nodeBody   []byte // 4088 bytes
}

func NewNode(page *pool.Page) (*Node, error) {
	pageData := page.GetPageData() // 4096 bytes
	if len(pageData) != disk.PageSize {
		return nil, fmt.Errorf("invalid page size: got %d, want %d", len(pageData), disk.PageSize)
	}

	return &Node{
		nodeUpdate: page.GetUpdateFlagRef(), // 1 byte
		nodeType:   pageData[:8],            // 8 bytes
		nodeBody:   pageData[8:],            // 4088 bytes
	}, nil
}

// GetRef関係 ======================================================================

func (n *Node) GetRefupdateFlag() *bool {
	return n.nodeUpdate
}

func (n *Node) GetRefnodeType() []byte {
	return n.nodeType
}

func (n *Node) GetRefnodeBody() []byte {
	return n.nodeBody
}

// Get関係 ======================================================================

func (n *Node) GetNodeType() NodeType {
	return nt(n.nodeType)
}

// Set関係 ======================================================================

func (n *Node) SetNodeType(nodeType NodeType) {
	if tmp, ok := nodeType.(nt); ok {
		*n.nodeUpdate = true
		copy(n.nodeType, tmp)
	}
}
