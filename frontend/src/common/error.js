import {Header} from "semantic-ui-react";
import React from "react";

export default function Error() {
    return <Header>
        <Header.Content>Unable to access this page</Header.Content>
        <Header.Subheader>Are you authorized to be here?</Header.Subheader>
    </Header>
}
