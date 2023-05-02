import React, {useEffect, useState} from "react";
import API from "../api";
import {Dropdown, Icon} from "semantic-ui-react";
import Cluster from "./index";

export function ClusterSelector({onSelect, user, trigger}) {
    const [clusters, setClusters] = useState(null)
    const [selected, setSelected] = useState(null)
    useEffect(async () => {
        const {clusters} = await API.getAuthorizedClusters()
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
    }, [user])

    const select = (cluster) => {
        setSelected(cluster.id)
        onSelect(cluster.entityName, cluster.tag)
    }

    const renderItem = (cluster) => {
        if (!cluster) return null
        const active = cluster.status === 'Active'
        return <Dropdown.Item disabled={!active} key={cluster.id} value={cluster.id} onClick={() => select(cluster)}>
            <Cluster cluster={cluster}/>
        </Dropdown.Item>
    }

    return <Dropdown trigger={trigger} icon={null} direction='left' loading={!clusters} value={selected}>
        <Dropdown.Menu>
            <Dropdown.Item disabled text={<span>Select a notebook executor</span>}/>
            {clusters ? clusters.map((cluster) => renderItem(cluster)) : null}
        </Dropdown.Menu>
    </Dropdown>
}