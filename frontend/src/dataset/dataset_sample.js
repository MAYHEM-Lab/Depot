import React, {useEffect, useState} from 'react';
import {Container, List, Loader, Tab, Table} from "semantic-ui-react";
import API from "../api";

const renderTable = (columns, sample) => {
    return (
        <Container className='dataset-sample-container'>
            <Table singleLine striped celled>
                <Table.Header>
                    <Table.Row>
                        {columns.map(col =>
                            <Table.HeaderCell key={col.name}>{col.name}</Table.HeaderCell>
                        )}
                    </Table.Row>
                </Table.Header>
                <Table.Body>
                    {!sample.length ?
                        <Table.Row><Table.Cell colSpan={columns.length} error textAlign='center'>There are not yet any materialized samples for this dataset.</Table.Cell></Table.Row>
                        :
                        sample.map((row, idx) =>
                            <Table.Row key={idx}>
                                {
                                    row.map((elem, idx) =>
                                        <Table.Cell key={idx}>
                                            <code>{elem}</code>
                                        </Table.Cell>
                                    )
                                }
                            </Table.Row>
                        )
                    }
                </Table.Body>
            </Table>
        </Container>
    )
}

const renderRaw = (sample) => {
    const panes = sample.map(([file, ...data]) => {
            return {
                menuItem: file,
                render: () => <Tab.Pane>
                    <List ordered>
                        {data.map((line, idx) => {
                            return <List.Item key={idx}><pre className='file-preview-line'>{line}</pre></List.Item>
                        })}
                    </List>
                </Tab.Pane>
            }
        }
    )
    return !sample.length ?
        <Table>
            <Table.Body>
                <Table.Row><Table.Cell error textAlign='center'>There are not yet any materialized samples for this dataset.</Table.Cell></Table.Row>
            </Table.Body>
        </Table> :
        <Tab panes={panes}/>

}

export default function DatasetSample({entity, dataset}) {
    const [sample, setSample] = useState(null)
    useEffect(() => {
        API.getSample(entity.name, dataset.tag).then(result => {
            setSample(result)
        })
    }, [entity, dataset])
    if (!sample) {
        return <Loader active/>
    } else {
        if (dataset.datatype.type === 'Table') {
            return renderTable(dataset.datatype.columns, sample)
        } else {
            return renderRaw(sample)
        }
    }
}
