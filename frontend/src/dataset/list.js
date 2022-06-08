import {Loader, Segment, Table} from "semantic-ui-react";
import DatasetIcon from "./dataset_icon";
import {Link} from "react-router-dom";
import util from "../util";
import React, {useEffect, useState} from "react";
import API from "../api";

export default function ListDatasets({entity}) {
    const [datasets, setDatasets] = useState(null)
    useEffect(() => {
        API.getDatasets(entity.name).then(setDatasets)
    }, [entity])
    return <DatasetTable entity={entity} datasets={datasets}/>
}

function DatasetTable({entity, datasets}) {
    return <Segment basic>
        <Loader active={!datasets}/>
        <Table celled singleLine>
            <Table.Header>
                <Table.Row>
                    <Table.HeaderCell/>
                    <Table.HeaderCell>Dataset</Table.HeaderCell>
                    <Table.HeaderCell>Creation Date</Table.HeaderCell>
                </Table.Row>
            </Table.Header>
            <Table.Body>
                {datasets ? datasets.map(dataset =>
                    <Table.Row key={dataset.tag}>
                        <Table.Cell textAlign='center'>
                            <DatasetIcon datatype={dataset.datatype}/>
                        </Table.Cell>
                        <Table.Cell>
                            <Link to={{pathname: `/${entity.name}/${dataset.tag}`}}>
                                <code>{dataset.tag}</code>
                            </Link>
                        </Table.Cell>
                        <Table.Cell>{util.formatTime(dataset.created_at)}</Table.Cell>
                    </Table.Row>
                ) : null}
            </Table.Body>
        </Table>
    </Segment>
}
