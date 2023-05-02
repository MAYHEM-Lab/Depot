import {Accordion, Header, List, Segment} from "semantic-ui-react";
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


function CauseEntry({cause}) {
    if (cause.type === 'TTL') {
        return <span>Retained by dataset TTL</span>
    } else if (cause.type === 'Dependency') {
        const {owner_name, dataset_name, segment_version} = cause
        return <span>Retained by dependent segment <Link to={`/${owner_name}/datasets/${dataset_name}/segments/${segment_version}`}>
            <code>{`${owner_name}/${dataset_name}:${segment_version}`}</code>
        </Link></span>
    } else if (cause.type === 'Manual') {
        const {entity_name} = cause
        return <span>Retained manually by <Link to={`/${entity_name}`}>
            <code>{entity_name}</code>
        </Link></span>
    }
}

function SegmentEntry({entity, owner, dataset, version, state, causes}) {
    return <div className='accordion-meta'>
        Segment <Link to={{pathname: `/${owner}/datasets/${dataset}/segments/${version}`}}>
        <code>{owner}/{dataset}:{version}</code>
    </Link>
        <SegmentState segmentState={state}/>
        <List bulleted>
            {causes.map((cause, idx) => {
                return <List.Item key={idx}>
                    <CauseEntry cause={cause}/>
                </List.Item>
            })}
        </List>
    </div>
}

function DatasetEntry({entity, owner, dataset, segments}) {
    const segmentPanels = segments.map(({version, size, state, causes}) => {
        return {
            key: version,
            title: {
                content: <span>
                    Version {version}
                    <span className='accordion-title-info'>{causes.length} reference{causes.length === 1 ? '' : 's'} - {util.formatBytes(size)}</span>
            </span>
            },
            content: {content: <SegmentEntry entity={entity} owner={owner} dataset={dataset} version={version} state={state} causes={causes}/>}
        }
    })
    return <>
        <div className='accordion-meta'>
            Dataset <Link to={{pathname: `/${owner}/datasets/${dataset}`}}>
            <code>{owner}/{dataset}</code>
        </Link>
        </div>
        <Accordion styled panels={segmentPanels}/>
    </>
}

function NotebooksListing({entity, notebooks}) {
    return <List>
        {notebooks
            .sort((a, b) => b.bytes - a.bytes)
            .map(nb => <List.Item key={nb.notebook_tag}>
                <NotebookEntry entity={entity} notebook={nb}/>
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

    const datasetPanels = grouped.map(({owner, dataset, segments}) => {
        const totalSize = segments.map(s => s.size).reduce((sum, s) => sum + s)
        return {
            key: `${owner}/${dataset}`,
            title: {
                content: <span><code>{owner}/{dataset}</code> <span className='accordion-title-info'>{segments.length} segments - {util.formatBytes(totalSize)}</span></span>
            },
            content: {content: <DatasetEntry entity={entity} owner={owner} dataset={dataset} segments={segments}/>}
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
    const uniqueSegments = [...new Map(
        quota.usage.storage.segments.map(s => [s.dataset_owner + '/' + s.dataset_tag + '/' + s.segment_version, s.bytes])
    ).values()];

    const datasets = uniqueSegments.reduce((a, b) => a + b, 0)
    const notebooks = quota.usage.storage.notebooks.map(n => n.bytes).reduce((a, b) => a + b, 0)

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