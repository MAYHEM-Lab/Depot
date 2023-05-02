import React from 'react';
import {Table} from "semantic-ui-react";

const renderTable = (columns) => {
    return (
        <Table singleLine basic='very'>
            <Table.Body>
                {columns.map(col =>
                    <Table.Row className='dataset-schema-field' key={col.name}>
                        <Table.Cell><code>{col.name}</code></Table.Cell>
                        <Table.Cell>{col.column_type.toLowerCase()}</Table.Cell>
                    </Table.Row>
                )}
            </Table.Body>
        </Table>
    )
}

const renderRaw = () => {
    return (<span>This dataset has raw data</span>)
}

export default function DatasetSchema({dataset}) {
    if (dataset.datatype.type === 'Table') {
        return renderTable(dataset.datatype.columns)
    } else {
        return renderRaw()
    }
}
