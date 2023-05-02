import React from 'react';
import {Icon} from "semantic-ui-react";

export default function DatasetIcon({datatype, color, size}) {
    if (datatype.type === 'Raw') {
        return <Icon name='file alternate outline' color={color} size={size}/>
    } else {
        return <Icon name='table' color={color} size={size}/>
    }
}
