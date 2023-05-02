import {Icon} from "semantic-ui-react";
import React from "react";

export default function Cluster({cluster}) {
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
            <Icon name='microchip'/>
            <span>4 vCPU</span>
            <span> &bull; </span>
            <span>16GB RAM</span>
        </div>
        <div className='cluster-select-description'>
            <Icon name='circle' color={active ? 'green' : 'orange'}/>
            {cluster.status}
        </div>
    </div>
}
