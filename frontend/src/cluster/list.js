import {Container, Icon, Item, Segment} from "semantic-ui-react";
import React, {useEffect, useState} from "react";
import API from "../api";

export default function ListClusters({entity}) {
    const [clusters, setClusters] = useState(null)

    useEffect(async () => {
        const {clusters} = await API.getClusters(entity.name)
        const clusterInfo = clusters.map(({cluster, owner}) => {
            return {
                tag: cluster.tag,
                id: cluster.id,
                entityName: owner.name,
                entityType: owner.type,
                status: cluster.status
            }
        })
        setClusters(clusterInfo)
    }, [entity])
    return <Container>
        <ClusterList entity={entity} clusters={clusters}/>
    </Container>
}

function ClusterList({entity, clusters}) {
    const renderItem = (cluster) => {
        if (!cluster) return null
        const active = cluster.status === 'Active'
        return <div key={cluster.id}>
            <div className='cluster-select-header'>
                <Icon name='server'/>
                <span className='cluster-select-title'>
                    {cluster.tag}
                </span>
            </div>
            <div className='cluster-select-description'>
                <Icon name={cluster.entityType === 'User' ? 'user' : 'building'}/>
                {cluster.entityName}
            </div>
            <div className='cluster-select-description'>
                <Icon name='circle' color={active ? 'green' : 'orange'}/>
                {cluster.status}
            </div>
        </div>
    }

    return <Segment basic loading={!clusters}>
        <Item.Group relaxed divided>
            {clusters ? clusters.map(cluster => {
                return renderItem(cluster)
            }) : null}
        </Item.Group>
    </Segment>
}

