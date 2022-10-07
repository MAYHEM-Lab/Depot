import {Container, Item, Segment} from "semantic-ui-react";
import React, {useEffect, useState} from "react";
import API from "../api";
import Cluster from "./index";

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
        <ClusterList clusters={clusters}/>
    </Container>
}

function ClusterList({clusters}) {
    const renderItem = (cluster) => {
        if (!cluster) return null
        return <Cluster key={cluster.id} cluster={cluster}/>
    }

    return <Segment basic loading={!clusters}>
        <Item.Group relaxed divided>
            {clusters ? clusters.map(cluster => {
                return renderItem(cluster)
            }) : null}
        </Item.Group>
    </Segment>
}

