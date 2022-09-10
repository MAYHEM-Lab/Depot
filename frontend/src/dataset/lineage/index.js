import React, {useEffect, useState} from 'react';
import ReactFlow, {Controls, Handle, Position} from "react-flow-renderer";
import {Container, Header, Loader} from "semantic-ui-react";
import {Link} from "react-router-dom";
import DatasetIcon from "../icon";

function DataNode({data: {renderLink, renderNode, entry, primary}}) {
    const color = entry.valid ? (primary ? 'blue' : 'black') : 'red';
    return (
        <Link to={renderLink(entry)}>
            <Header
                size='tiny'
                icon
                className='dataset-lineage-node'
            >
                <DatasetIcon datatype={entry.datatype} color={color}/>
                <Handle className='dataset-lineage-handle' type="target" position={Position.Left}/>
                {renderNode(entry)}
                <Handle className='dataset-lineage-handle' type="source" position={Position.Right}/>
            </Header>
        </Link>
    )
}

const nodeTypes = {dataset: DataNode}

export default function DataGraph({graphPromise, renderLink, renderNode, getNodeId, centerId}) {
    const [graph, setGraph] = useState(null)
    useEffect(() => {
        graphPromise().then(result => {
            const nodes = []
            const edges = []
            const loop = (entry, parent = null, x = 0, y = 0,) => {
                nodes.push({
                    id: getNodeId(entry),
                    type: 'dataset',
                    data: {
                        renderLink: renderLink,
                        renderNode: renderNode,
                        entry: entry,
                        primary: getNodeId(entry) === centerId
                    },
                    primary: true,
                    position: {x: x, y: y},
                    draggable: false,
                    selectable: false,
                    className: 'dataset-lineage-edge',
                    connectable: false
                })
                if (parent) {
                    edges.push({
                        id: `${getNodeId(parent)}-${getNodeId(entry)}`,
                        type: 'straight',
                        target: getNodeId(parent),
                        source: getNodeId(entry),
                        animated: entry.valid,
                        className: 'dataset-lineage-edge',
                        style: {
                            strokeWidth: 4,
                            stroke: entry.valid ? null : 'red'
                        },
                        markerEnd: {
                            type: entry.valid ? 'arrowclosed' : null
                        }
                    })
                }
                entry.from.forEach((elem, idx) =>
                    loop(elem, entry, x - 250, idx * 100)
                )
            }
            loop(result)
            setGraph({nodes: nodes, edges: edges})
        })
    }, [centerId])

    return (
        <Container className='dataset-lineage-container'>
            <Loader active={!graph}/>
            {graph ? <ReactFlow
                nodeTypes={nodeTypes}
                nodes={graph.nodes}
                edges={graph.edges}
                onNodeClick={() => {}}
                fitView
                proOptions={{account: "paid-custom", hideAttribution: true}}
            >
                <Controls showZoom showInteractive={false}/>
            </ReactFlow> : null
            }
        </Container>
    )
}
