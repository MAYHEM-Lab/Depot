import {Container, Header, Item, Segment} from "semantic-ui-react";
import DatasetIcon from "./icon";
import {Link} from "react-router-dom";
import util from "../util";
import React, {useContext, useEffect, useState} from "react";
import API from "../api";
import VisibilityInfo from "./visbility";
import {EventContext} from "../common/bus";

export default function ListDatasets({entity}) {
    const [datasets, setDatasets] = useState(null)
    const [trigger, setTrigger] = useState(0)
    const eventBus = useContext(EventContext)

    const invalidate = () => setTrigger(t => t + 1)

    useEffect(() => {
        eventBus.on('reload-datasets', invalidate)
        return () => eventBus.remove('reload-datasets', invalidate)
    })

    useEffect(async () => {
        const datasets = await API.getDatasets(entity.name)
        setDatasets(datasets.map(d => {
            return {...d, owner: entity.name}
        }))
    }, [entity, trigger])
    return <Container>
        <DatasetList full={false} datasets={datasets}/>
    </Container>
}

export function RecentDatasets() {
    const [datasets, setDatasets] = useState()
    const [trigger, setTrigger] = useState(0)
    const eventBus = useContext(EventContext)

    const invalidate = () => setTrigger(t => t + 1)

    useEffect(() => {
        eventBus.on('reload-datasets', invalidate)
        return () => eventBus.remove('reload-datasets', invalidate)
    })

    useEffect(async () => {
        const {datasets, notebooks, entities} = await API.getHomePage()
        setDatasets(datasets.map(d => {
            const entity = entities.find(e => e.id === d.owner_id)
            return {...d, owner: entity.name}
        }))
    }, [trigger])
    return <Container>
        <DatasetList full={true} datasets={datasets}/>
    </Container>
}

function DatasetList({full, datasets}) {
    return <Segment basic loading={!datasets}>
        <Item.Group relaxed divided>
            {datasets ? datasets.map(dataset => {
                return <Item key={dataset.id}>
                    <DatasetIcon datatype={dataset.datatype} size='big'/>
                    <Item.Content>
                        <Item.Header>
                            <Header>
                                <Header.Content>
                                    {full ?
                                        <>
                                            <Link to={`/${dataset.owner}`}>{dataset.owner}</Link>
                                            <span className='resource-divider'>/</span>
                                        </>
                                        : null
                                    }
                                    <Link to={`/${dataset.owner}/datasets/${dataset.tag}`}>{dataset.tag}</Link>
                                </Header.Content>
                                <VisibilityInfo dataset={dataset}/>
                            </Header>
                        </Item.Header>
                        <Item.Meta>{dataset.origin}</Item.Meta>
                        <Item.Description>
                            <p className={'dataset-list-description' + (dataset.description ? '' : ' missing')}>
                                {dataset.description || 'No description provided.'}
                            </p>
                        </Item.Description>
                        <Item.Extra>Created on {util.formatTime(dataset.created_at)}</Item.Extra>
                    </Item.Content>
                </Item>
            }) : null}
        </Item.Group>
    </Segment>
}

