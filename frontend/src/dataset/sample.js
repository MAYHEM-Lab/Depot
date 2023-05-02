import React, {useEffect, useState} from 'react';
import {Container, Label, List, Loader, Tab, Table} from "semantic-ui-react";
import API from "../api";
import util from "../util";

const renderTable = (columns, sample, rows) => {
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
    const panes = sample.map(([file, size, type, data], idx) => {
        const text = window.atob(data)
        const lines = text.split(/\n/)
        return {
            menuItem: {
                content: <><span className='file-preview-name' title={file}>{file}</span><span className='file-preview-size'> &mdash; {util.formatBytes(size)}</span></>,
                key: idx
            },
            render: () => <Tab.Pane className='file-preview-container'>
                <Label attached='top'>Displayed {text.length} bytes of {util.formatBytes(size)}</Label>
                <List ordered>
                    {lines.map((line, idx) => {
                        return <List.Item key={idx}>
                            <pre className='file-preview-line'>{line}&#x200b;</pre>
                        </List.Item>
                    })}
                </List>
            </Tab.Pane>
        }
    })
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
    const [rows, setRows] = useState(null)
    useEffect(async () => {
        const {sample, rows} = await API.getSample(entity.name, dataset.tag)
        setSample(sample)
        setRows(rows)
    }, [entity, dataset])
    if (!sample) {
        return <Loader active/>
    } else {
        if (dataset.datatype.type === 'Table') {
            return renderTable(dataset.datatype.columns, sample, rows)
        } else {
            return renderRaw(sample)
        }
    }
}
