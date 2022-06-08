import React, {useEffect, useState} from 'react';
import {Link, useOutletContext, useParams} from "react-router-dom";

import API from '../api'
import {Container, Grid, Header, Icon, Loader} from "semantic-ui-react";
import DataGraph from "./lineage";
import SegmentTimeline from "./segment_timeline";

export default function SegmentInfo() {
    const {version} = useParams()
    const {entity, dataset} = useOutletContext();
    const [segment, setSegment] = useState(null)
    useEffect(() => {
        API.getSegment(entity.name, dataset.tag, version).then(segment => setSegment(segment));
    }, [entity, dataset, version])

    return <div>
        <Link to={`/${entity.name}/${dataset.tag}/segments`}><Icon name='arrow left'/>View all segments</Link>
        <Container className='segment-info-container'>
        {!segment ? <Loader active/> :
            <Grid>
                <Grid.Row columns={1}>
                    <Grid.Column>
                        <Header size='small'>Timeline</Header>
                        <SegmentTimeline version={version}/>
                    </Grid.Column>
                </Grid.Row>
                <Grid.Row columns={1}>
                    <Grid.Column>
                        <Header size='small'>Lineage</Header>
                        <DataGraph
                            graphPromise={API.getProvenance(entity.name, dataset.tag, version)}
                            renderLink={(e) => `/${e.entity_name}/${e.dataset_tag}/segments/${e.version}`}
                            renderNode={(e) => `${e.entity_name}/${e.dataset_tag}:${e.version}`}
                            getNodeId={(e) => `${e.entity_name}/${e.dataset_tag}:${e.version}`}
                            centerId={`${entity.name}/${dataset.tag}:${version}`}
                        />
                    </Grid.Column>
                </Grid.Row>
            </Grid>
        }
        </Container>
    </div>
}
