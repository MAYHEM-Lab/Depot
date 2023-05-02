import {Label} from "semantic-ui-react";
import React from "react";

export default function VisibilityInfo({dataset}) {
    return <Label basic color={dataset.visibility === 'Public' ? 'green' : 'orange'}>{dataset.visibility}</Label>
}