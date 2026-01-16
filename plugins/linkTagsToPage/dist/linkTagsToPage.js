"use strict";
(function () {
    const PluginApi = window.PluginApi;
    PluginApi.patch.before('TagLink', (props) => {
        return [{ ...props, linkType: 'details' }];
    });
})();
//# sourceMappingURL=linkTagsToPage.js.map