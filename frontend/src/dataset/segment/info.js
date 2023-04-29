import React, {useEffect, useRef, useState} from 'react';
import {Link, useOutletContext, useParams} from "react-router-dom";

import API from '../../api'
import {Button, Container, Header, Icon, Loader} from "semantic-ui-react";
import DataGraph from "../lineage";
import SegmentTimeline from './timeline';

export default function SegmentInfo() {
    const {version} = useParams()
    const {entity, dataset, owner, invalidate} = useOutletContext();
    const [segment, setSegment] = useState(null)
    const [matRequested, setMatRequested] = useState(false)
    const [matLoading, setMatLoading] = useState(false)
    const [bundleLoading, setBundleLoading] = useState(false)

    const downloadFrame = useRef(null)
    useEffect(() => {
        API.getSegment(entity.name, dataset.tag, version).then(segment => setSegment(segment));
    }, [entity, dataset, version])

    const requestMaterialization = async () => {
        setMatLoading(true)
        setMatRequested(true)
        if (dataset.origin == 'Streaming') {
            try {
            await API.materializeStreamingSegment(entity.name, dataset.tag, version)
            } finally {
                setMatLoading(false)
            }
        } else {
        try {
            await API.materializeSegment(entity.name, dataset.tag, version)
        } finally {
            setMatLoading(false)
        }
    }
    }

    const requestDownload = async () => {
        setBundleLoading(true)
        try {
            downloadFrame.current.src = await API.bundleSegment(entity.name, dataset.tag, version)
        } finally {
            setBundleLoading(false)
        }
    }

    return <div>
        <iframe ref={downloadFrame} style={{display: 'none'}}/>
        <Link to={`/${entity.name}/datasets/${dataset.tag}/segments`}><Icon name='arrow left'/>View all segments</Link>
        {segment ?
            <>
                {segment.state === 'Materialized' ?
                    <Button size='small' primary icon='cloud download' loading={bundleLoading} disabled={bundleLoading} floated='right' onClick={requestDownload}/> :
                    null
                }

                {owner && segment.state === 'Announced' ?
                    <Button size='small' positive floated='right' loading={matLoading} disabled={matRequested} onClick={requestMaterialization}>Materialize</Button> :
                    null
                }
            </> : null
        }
        <Container className='segment-info-container'>
            {!segment ? <Loader active/> :
                <>
                    <Header size='small'>Timeline</Header>
                    <SegmentTimeline version={version}/>
                    <Header size='small'>Lineage</Header>
                    <DataGraph
                        graphPromise={() => API.getProvenance(entity.name, dataset.tag, version)}
                        renderLink={(e) => `/${e.entity_name}/datasets/${e.dataset_tag}/segments/${e.version}`}
                        renderNode={(e) => `${e.entity_name}/${e.dataset_tag}:${e.version}`}
                        getNodeId={(e) => `${e.entity_name}/${e.dataset_tag}:${e.version}`}
                        centerId={`${entity.name}/${dataset.tag}:${version}`}
                    />
                </>
            }
        </Container>
    </div>
}
