import {Segment} from "semantic-ui-react";
import React, {useEffect, useState} from "react";
import API from "../api";
import './quota.css'
import util from "../util";

const COLORS = {
    'Notebooks': '#ffcc00',
    'Datasets': '#4cd964'
}

export default function QuotaUsage({entity}) {
    const [quota, setQuota] = useState(null)

    useEffect(async () => {
        const quota = await API.getQuota(entity.name)
        setQuota(quota)
    }, [entity])
    return <Segment basic loading={!quota}>
        {quota ? Storage(quota) : null}
    </Segment>
}

function Storage(quota) {
    const notebooks = quota.usage.storage.notebooks.map(n => n.bytes).reduce((a, b) => a + b, 0)
    const datasets = quota.usage.storage.segments.map(n => n.bytes).reduce((a, b) => a + b, 0)
    const consumed = {
        'Notebooks': notebooks,
        'Datasets': datasets,
    }
    const size = quota.allocation.storage.allocated_bytes
    return <div className='quota-storage'>
        <h3>
            Storage
            <span className='quota-storage-text'>{util.formatBytes(notebooks + datasets) + ' of ' + util.formatBytes(size) + ' used'}</span>
        </h3>

        <div className='quota-storage-bar'>
            <span className='value' style={{width: `${Math.round(100 * notebooks / size)}%`, backgroundColor: '#ffcc00'}}/>
            <span className='value' style={{width: `${Math.round(100 * datasets / size)}%`, backgroundColor: '#4cd964'}}/>
        </div>

        <div className='quota-storage-legend'>
            {
                Object.keys(COLORS).map(item => {
                    return <div key={item}>
                        <span className='quota-storage-legend-dot' style={{backgroundColor: COLORS[item]}}/>
                        {item}
                        <span className='quota-storage-legend-meta'> - {util.formatBytes(consumed[item])}</span>
                    </div>
                })
            }
        </div>
    </div>
}