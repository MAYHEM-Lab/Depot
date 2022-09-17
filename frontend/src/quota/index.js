import {Accordion, List, Segment} from "semantic-ui-react";
import React, {useEffect, useState} from "react";
import API from "../api";
import './quota.css'
import util from "../util";
import {Link} from "react-router-dom";
import SegmentState from "../dataset/segment/state";

export default function QuotaUsage({entity}) {
    const [quota, setQuota] = useState(null)

    useEffect(async () => {
        const quota = await API.getQuota(entity.name)
        setQuota(quota)
    }, [entity])
    return <Segment basic loading={!quota}>
        {quota ? Storage(entity, quota) : null}
    </Segment>
}

function NotebookEntry({entity, notebook}) {
    return <>
        <Link to={`/${entity.name}/notebooks/${notebook.notebook_tag}`}>
            <code>{`${entity.name}/${notebook.notebook_tag}`}</code>
        </Link>
        <span> - </span>
        <span className='quota-storage-legend-meta'>{util.formatBytes(notebook.bytes)}</span>
    </>
}

function SegmentEntry({entity, causes}) {
    return JSON.stringify(causes)
}

function DatasetEntry({entity, segments}) {
    const segmentPanels = segments.map(({version, size, state, causes}) => {
        return {
            key: version,
            title: {
                content: <span>
                    Version {version} - {util.formatBytes(size)}
            </span>
            },
            content: {content: <SegmentEntry entity={entity} causes={causes}/>}
        }
    })
    return <>
        <div>
            Dataset kc/test
        </div>
        <Accordion styled panels={segmentPanels}/>
    </>
}

function NotebooksListing({entity, notebooks}) {
    return <List>
        {notebooks
            .sort((a, b) => b.bytes - a.bytes)
            .map(nb => <List.Item>
                <NotebookEntry entity={entity} key={nb.notebook_tag} notebook={nb}/>
            </List.Item>)
        }
    </List>
}

function SegmentsListing({entity, segments}) {
    const byDataset = segments.reduce((l, s) => {
        const dataset = `${s.dataset_owner}/${s.dataset_tag}`
        const segment = {
            size: s.bytes,
            state: s.state,
            version: s.segment_version,
            cause: s.cause
        }
        return {...l, [dataset]: (l[dataset] || []).concat(segment)}
    }, {})
    const grouped = Object.keys(byDataset).map(tag => {
        const [owner, dataset] = tag.split('/')
        const refs = byDataset[tag]
        const byVersion = refs.reduce((l, r) => {
            const version = r.version
            return {...l, [version]: (l[version] || []).concat(r)}
        }, {})
        const versions = Object.keys(byVersion).map(version => {
            const vs = byVersion[version]
            const causes = vs.map(c => c.cause)
            const size = vs[0].size
            const state = vs[0].state
            return {version: version, causes: causes, size: size, state: state}
        })
        return {owner: owner, dataset: dataset, segments: versions}
    })
    console.log(grouped)

    const datasetPanels = grouped.map(({owner, dataset, segments}) => {
        return {
            key: `${owner}/${dataset}`,
            title: {content: `${owner}/${dataset}`},
            content: {content: <DatasetEntry entity={entity} segments={segments}/>}
        }
    })
    return <Accordion panels={datasetPanels} styled/>
}

function StorageQuotaItem({color, name, size}) {
    return <>
        <span className='quota-storage-legend-dot' style={{backgroundColor: color}}/>
        {name}
        <span className='quota-storage-legend-meta'> - {util.formatBytes(size)}</span>
    </>
}

function Storage(entity, quota) {
    const notebooks = quota.usage.storage.notebooks.map(n => n.bytes).reduce((a, b) => a + b, 0)
    const datasets = quota.usage.storage.segments.map(n => n.bytes).reduce((a, b) => a + b, 0)

    const size = quota.allocation.storage.allocated_bytes

    const panels = [
        {
            key: 'notebook-legend',
            title: {content: <StorageQuotaItem name='Notebooks' color='#ffcc00' size={notebooks}/>},
            content: {content: <NotebooksListing entity={entity} notebooks={quota.usage.storage.notebooks}/>}
        },
        {
            key: 'dataset-legend',
            title: {content: <StorageQuotaItem name='Segments' color='#4cd964' size={datasets}/>},
            content: {content: <SegmentsListing entity={entity} segments={quota.usage.storage.segments}/>}
        }
    ]

    return <div className='quota-storage'>
        <h3>
            Storage
            <span
                className='quota-storage-text'>{util.formatBytes(notebooks + datasets) + ' of ' + util.formatBytes(size) + ' used'}</span>
        </h3>

        <div className='quota-storage-bar'>
            <span className='value'
                  style={{width: `${Math.round(100 * notebooks / size)}%`, backgroundColor: '#ffcc00'}}/>
            <span className='value'
                  style={{width: `${Math.round(100 * datasets / size)}%`, backgroundColor: '#4cd964'}}/>
        </div>

        <Accordion panels={panels}/>
    </div>
}