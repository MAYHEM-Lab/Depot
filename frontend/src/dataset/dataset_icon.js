import React from 'react';
import {Icon} from "semantic-ui-react";

export default function DatasetIcon({datatype, color}) {
    if (datatype.type === 'Raw') {
        return <Icon name='file alternate outline' color={color}/>
    } else {
        return <Icon name='table' color={color}/>
    }
}
