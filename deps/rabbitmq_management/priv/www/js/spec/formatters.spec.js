describe("matcher", () => {
    var items

    beforeEach(() => {
        items = [{name: "item1"}, {name: "item2"}, {name: "item3"}]
    })

    describe("when the criteria are empty", () => {
        it("an empty string should match any input", () => {
            expect(items.filter(matcher(''))).toEqual(items)
        })

        it("empty criteria should match any input", () => {
            expect(items.filter(matcher())).toEqual(items)
        })
    })

    describe("when the filter is a string", () => {
        it("should return the items with matching name, case insensitive", () => {
            expect(items.filter(matcher('Item2'))).toEqual([{name: "item2"}])
        })
    })

    describe("when the filter is a regexp", () => {
        it("should return the items which match the regexp", () => {
            expect(items.filter(matcher('',/item[1-2]/))).toEqual([{name: "item1"}, {name: "item2"}])
        })
    })
})

describe("filter_ui", () => {
    var items
    var expectedOutput
    var expectedItems

    beforeEach(() => {
        items = [{name: 'guest', password_hash: '3n3NRVci6Doy6NlYWV7kMUAfvtLvpAO21P35vBczYMj6gjCn', hashing_algorithm: 'rabbit_password_hashing_sha256', tags: Array(1), limits: {}}, {name: 'user1', password_hash: 'NP83tWswsrzxSs6ekusMIa80OwCCkJVQO1vyQx8CcEKp3nmn', hashing_algorithm: 'rabbit_password_hashing_sha256', tags: Array(0), limits: {}}, {name: 'user2', password_hash: 'kdOVV/VZwBPznRtKs24ms8PnSso39m5nrY7WFFM2X7yqrvS4', hashing_algorithm: 'rabbit_password_hashing_sha256', tags: Array(0), limits: {}}, {name: 'user3', password_hash: '7LFGMPFg+noPPyz2Vv/cCGYoIZTLeyVwYrAc5Is1iyj3udHZ', hashing_algorithm: 'rabbit_password_hashing_sha256', tags: Array(0), limits: {}}, {name: 'user4', password_hash: '2FpxRT7P9/8TrvQWT+RB1VcmMTzvMlwbEmeSIR8VGzc3BMiF', hashing_algorithm: 'rabbit_password_hashing_sha256', tags: Array(0), limits: {}}, {name: 'user5', password_hash: 'XWoDWb1jRcuKsemy1fE8zcN3DIbXP4tnszCxk0/cDdarssr+', hashing_algorithm: 'rabbit_password_hashing_sha256', tags: Array(0), limits: {}}, {name: 'user6', password_hash: 'QeUtCojSwqKrllAFO0TWvxJcXVFz5Y35F+r132h8AAJ7V1Mv', hashing_algorithm: 'rabbit_password_hashing_sha256', tags: Array(0), limits: {}}]
        expectedItems = items

        store_pref('truncate', '100')
    })

    describe("when there is no filter", () => {
        beforeEach(() => {
            expectedOutput = '<div class="filter"><table><tr><th>Filter:</th><td><input id="filter" type="text" value=""/><input type="checkbox" name="filter-regex-mode" id="filter-regex-mode"/><label for="filter-regex-mode">Regex</label> <span class="help" id="filter-regex"></span></td></tr></table><p id="filter-truncate"><span class="updatable">7 items, page size up to </span><input type="text" id="truncate" value="100"></p></div>'
            current_filter = ''
            current_filter_regex = false
            current_filter_regex_on = false
        })

        it("should return the items", () => {
            expect(filter_ui(items)).toEqual(expectedOutput)
            expect(items).toEqual(expectedItems)
        })

        describe("when there is more than one page of items", () => {
            beforeEach(() => {
                store_pref('truncate', '5')
                expectedOutput = '<div class="filter"><table><tr><th>Filter:</th><td><input id="filter" type="text" value=""/><input type="checkbox" name="filter-regex-mode" id="filter-regex-mode"/><label for="filter-regex-mode">Regex</label> <span class="help" id="filter-regex"></span></td></tr></table><p id="filter-truncate"><span class="updatable">5 items<span id="filter-warning-show"> (only showing first</span> </span><input type="text" id="truncate" value="5"></p></div>'
                expectedItems = [{name: 'guest', password_hash: '3n3NRVci6Doy6NlYWV7kMUAfvtLvpAO21P35vBczYMj6gjCn', hashing_algorithm: 'rabbit_password_hashing_sha256', tags: Array(1), limits: {}}, {name: 'user1', password_hash: 'NP83tWswsrzxSs6ekusMIa80OwCCkJVQO1vyQx8CcEKp3nmn', hashing_algorithm: 'rabbit_password_hashing_sha256', tags: Array(0), limits: {}}, {name: 'user2', password_hash: 'kdOVV/VZwBPznRtKs24ms8PnSso39m5nrY7WFFM2X7yqrvS4', hashing_algorithm: 'rabbit_password_hashing_sha256', tags: Array(0), limits: {}}, {name: 'user3', password_hash: '7LFGMPFg+noPPyz2Vv/cCGYoIZTLeyVwYrAc5Is1iyj3udHZ', hashing_algorithm: 'rabbit_password_hashing_sha256', tags: Array(0), limits: {}}, {name: 'user4', password_hash: '2FpxRT7P9/8TrvQWT+RB1VcmMTzvMlwbEmeSIR8VGzc3BMiF', hashing_algorithm: 'rabbit_password_hashing_sha256', tags: Array(0), limits: {}}]
            })

            it("should return the first page of items", () => {
                expect(filter_ui(items)).toEqual(expectedOutput)
                expect(items).toEqual(expectedItems)
            })
        })
    })

    describe("when there is a filter", () => {
        beforeEach(() => {
            expectedOutput = '<div class="filter"><table class="filter-active"><tr><th>Filter:</th><td><input id="filter" type="text" value="user"/><input type="checkbox" name="filter-regex-mode" id="filter-regex-mode"/><label for="filter-regex-mode">Regex</label> <span class="help" id="filter-regex"></span></td></tr></table><p id="filter-truncate"><span class="updatable">6 of 7 items selected, page size up to </span><input type="text" id="truncate" value="100"></p></div>'
            current_filter = 'user'
            current_filter_regex = false
            current_filter_regex_on = false
            expectedItems = [{name: 'user1', password_hash: 'NP83tWswsrzxSs6ekusMIa80OwCCkJVQO1vyQx8CcEKp3nmn', hashing_algorithm: 'rabbit_password_hashing_sha256', tags: Array(0), limits: {}}, {name: 'user2', password_hash: 'kdOVV/VZwBPznRtKs24ms8PnSso39m5nrY7WFFM2X7yqrvS4', hashing_algorithm: 'rabbit_password_hashing_sha256', tags: Array(0), limits: {}}, {name: 'user3', password_hash: '7LFGMPFg+noPPyz2Vv/cCGYoIZTLeyVwYrAc5Is1iyj3udHZ', hashing_algorithm: 'rabbit_password_hashing_sha256', tags: Array(0), limits: {}}, {name: 'user4', password_hash: '2FpxRT7P9/8TrvQWT+RB1VcmMTzvMlwbEmeSIR8VGzc3BMiF', hashing_algorithm: 'rabbit_password_hashing_sha256', tags: Array(0), limits: {}}, {name: 'user5', password_hash: 'XWoDWb1jRcuKsemy1fE8zcN3DIbXP4tnszCxk0/cDdarssr+', hashing_algorithm: 'rabbit_password_hashing_sha256', tags: Array(0), limits: {}}, {name: 'user6', password_hash: 'QeUtCojSwqKrllAFO0TWvxJcXVFz5Y35F+r132h8AAJ7V1Mv', hashing_algorithm: 'rabbit_password_hashing_sha256', tags: Array(0), limits: {}}]
        })

        it("should return the matching items", () => {
            expect(filter_ui(items)).toEqual(expectedOutput)
            expect(items).toEqual(expectedItems)
        })

        describe("when there is more than one page of items", () => {
            beforeEach(() => {
                store_pref('truncate', '5')
                expectedOutput = '<div class="filter"><table><tr><th>Filter:</th><td><input id="filter" type="text" value=""/><input type="checkbox" name="filter-regex-mode" id="filter-regex-mode"/><label for="filter-regex-mode">Regex</label> <span class="help" id="filter-regex"></span></td></tr></table><p id="filter-truncate"><span class="updatable">6 of 7 items slected<span id="filter-warning-show"> (only showing first</span> </span><input type="text" id="truncate" value="5"></p></div>'
                expectedItems = [{name: 'user1', password_hash: 'NP83tWswsrzxSs6ekusMIa80OwCCkJVQO1vyQx8CcEKp3nmn', hashing_algorithm: 'rabbit_password_hashing_sha256', tags: Array(0), limits: {}}, {name: 'user2', password_hash: 'kdOVV/VZwBPznRtKs24ms8PnSso39m5nrY7WFFM2X7yqrvS4', hashing_algorithm: 'rabbit_password_hashing_sha256', tags: Array(0), limits: {}}, {name: 'user3', password_hash: '7LFGMPFg+noPPyz2Vv/cCGYoIZTLeyVwYrAc5Is1iyj3udHZ', hashing_algorithm: 'rabbit_password_hashing_sha256', tags: Array(0), limits: {}}, {name: 'user4', password_hash: '2FpxRT7P9/8TrvQWT+RB1VcmMTzvMlwbEmeSIR8VGzc3BMiF', hashing_algorithm: 'rabbit_password_hashing_sha256', tags: Array(0), limits: {}}, {name: 'user5', password_hash: 'XWoDWb1jRcuKsemy1fE8zcN3DIbXP4tnszCxk0/cDdarssr+', hashing_algorithm: 'rabbit_password_hashing_sha256', tags: Array(0), limits: {}}]
            })

            it("should return the first page of matching items", () => {
                expect(filter_ui(items)).toEqual(expectedOutput)
                expect(items).toEqual(expectedItems)
            })
        })
    })
})
