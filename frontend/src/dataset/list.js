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

    useEffect(() => {
        API.getDatasets(entity.name).then(setDatasets)
    }, [entity, trigger])
    return <Container>
        <DatasetList entity={entity} datasets={datasets}/>
    </Container>
}

function DatasetList({entity, datasets}) {
    return <Segment basic loading={!datasets}>
        <Item.Group relaxed divided>
            {datasets ? datasets.map(dataset => {
                return <Item key={dataset.id}>
                    <DatasetIcon datatype={dataset.datatype} size='big'/>
                    <Item.Content>
                        <Item.Header>
                            <Header>
                                <Header.Content>
                                    <Link to={{pathname: `/${entity.name}/datasets/${dataset.tag}`}}>
                                        <code>{dataset.tag}</code>
                                    </Link>
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

