import React from "react";

const eventBus = {
    on(event, handler) {
        document.addEventListener(event, (e) => handler(e.detail));
    },
    dispatch(event, data) {
        document.dispatchEvent(new CustomEvent(event, { detail: data }));
    },
    remove(event, handler) {
        document.removeEventListener(event, handler);
    },
};

export const EventContext = React.createContext(eventBus)
