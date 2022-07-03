import React from 'react';
import {Container, Grid, Header} from "semantic-ui-react";
import DatasetSchema from "./dataset_schema";
import DatasetStats from "./dataset_stats";
import DatasetSample from "./dataset_sample";
import {useOutletContext} from "react-router";
import DataGraph from "./lineage";
import API from "../api";

export default function DatasetInfo() {
    const {entity, dataset} = useOutletContext();
    return (
        <Grid>
            <Grid.Row columns={1}>
                <Grid.Column>
                    <Container fluid>
                        <Header size='small'>Description</Header>
                        <p className={'dataset-description' + (dataset.description ? '' : ' missing')}>
                            {dataset.description || 'No description provided.'}
                        </p>
                    </Container>
                </Grid.Column>
            </Grid.Row>
            <Grid.Row columns={2}>
                <Grid.Column>
                    <Header size='small'>Information</Header>
                    <DatasetStats entity={entity} dataset={dataset}/>
                </Grid.Column>
                <Grid.Column>
                    <Header size='small'>Schema</Header>
                    <DatasetSchema entity={entity} dataset={dataset}/>
                </Grid.Column>
            </Grid.Row>
            <Grid.Row columns={1}>
                <Grid.Column>
                    <Header size='small'>Lineage</Header>
                    <DataGraph
                        graphPromise={() => API.getLineage(entity.name, dataset.tag)}
                        renderLink={(e) => `/${e.entity_name}/${e.dataset_tag}`}
                        renderNode={(e) => `${e.entity_name}/${e.dataset_tag}`}
                        getNodeId={(e) => `${e.entity_name}/${e.dataset_tag}`}
                        centerId={`${entity.name}/${dataset.tag}`}
                    />
                </Grid.Column>
            </Grid.Row>
            <Grid.Row columns={1}>
                <Grid.Column>
                    <Header size='small'>Sample Data</Header>
                    <DatasetSample entity={entity} dataset={dataset}/>
                </Grid.Column>
            </Grid.Row>
        </Grid>
    )
}
