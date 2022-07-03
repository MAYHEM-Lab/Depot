import React, {useEffect, useState} from 'react';
import {Link, useOutletContext, useParams} from "react-router-dom";

import API from '../api'
import {Button, Container, Header, Icon, Loader} from "semantic-ui-react";
import DataGraph from "./lineage";
import SegmentTimeline from "./segment_timeline";

export default function SegmentInfo() {
    const {version} = useParams()
    const {entity, dataset, owner} = useOutletContext();
    const [segment, setSegment] = useState(null)
    const [matRequested, setMatRequested] = useState(false)
    const [matLoading, setMatLoading] = useState(false)
    useEffect(() => {
        API.getSegment(entity.name, dataset.tag, version).then(segment => setSegment(segment));
    }, [entity, dataset, version])

    const requestMaterialization = async () => {
        setMatLoading(true)
        setMatRequested(true)
        try {
            await API.materializeSegment(entity.name, dataset.tag, version)
        } catch (e) {

        } finally {
            setMatLoading(false)
        }
    }

    return <div>
        <Link to={`/${entity.name}/${dataset.tag}/segments`}><Icon name='arrow left'/>View all segments</Link>
        {segment ?
            <>
                {owner && segment.state === 'Materialized' ? <Button size='small' primary icon='cloud download' floated='right'/> : null}
                {/*{owner && segment.state !== 'Released' ? <Button size='small' negative floated='right'>Release</Button> : null}*/}
                {owner && segment.state === 'Announced' ?
                    <Button size='small' positive floated='right' loading={matLoading} disabled={matRequested} onClick={requestMaterialization}>Materialize</Button> : null}
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
                        renderLink={(e) => `/${e.entity_name}/${e.dataset_tag}/segments/${e.version}`}
                        renderNode={(e) => `${e.entity_name}/${e.dataset_tag}:${e.version}`}
                        getNodeId={(e) => `${e.entity_name}/${e.dataset_tag}:${e.version}`}
                        centerId={`${entity.name}/${dataset.tag}:${version}`}
                    />
                </>
            }
        </Container>
    </div>
}
