const jsdom = require('jsdom')
const { JSDOM } = jsdom

const http = require('http')
var enableDestroy = require('server-destroy')

describe("login", () => {
    var server
    var authResponse
    beforeAll(() => {
        var authListener = (req,res) => {
            res.setHeader("Content-Type", "application/json")
            res.writeHead(200)
            res.end(authResponse)

        }
        server = http.createServer(authListener)
        server.listen(15672)
        enableDestroy(server)
    })

    afterAll(() => {
        server.destroy()
    })

    describe("outer", () => {
        it("should have an outer block", () => {
            JSDOM.fromFile("../index.html").then(dom => {
                expect(dom.window.document.getElementById('outer')).toBeDefined()
            })
        })
    })

    describe("when OAuth is not enabled", () => {
        beforeEach(() => {
            authResponse = `{
                "enable_oauth": false,
                "enable_uaa": false,
                "client_id": "",
                "oauth_location": "",
                "uaa_client_id": "",
                "uaa_location": ""
            }`
        })

        it("should render the login form", () => {
            JSDOM.fromFile("../index.html", { runScripts: "dangerously" }).then(dom => {
                const login = dom.window.document.getElementById('login')
                expect(login).toBeDefined()
                const loginStatus = dom.window.document.getElementById('login-status')
                expect(loginStatus).toBeDefined()
                const inputs = [...dom.window.document.querySelectorAll('input')]
                expect(inputs).toHaveSize(3)
                expect(inputs[0].type).toEqual("text")
                expect(inputs[1].type).toEqual("password")
                expect(inputs[2].type).toEqual("submit")
            })
        })
    })

    describe("when OAuth is enabled", () => {
        beforeEach(() => {
            authResponse = `{
                "enable_oauth": true,
                "enable_uaa": false,
                "client_id": "validID",
                "oauth_location": "someURL",
                "uaa_client_id": "",
                "uaa_location": ""
            }`
        })

        describe("when the OAuth provider is UAA", () => {
            beforeEach(() => {
                authResponse = `{
                    "enable_oauth": true,
                    "enable_uaa": true,
                    "client_id": "",
                    "oauth_location": "",
                    "uaa_client_id": "validID",
                    "uaa_location": "someURL"
                }`
            })

            it("should render the link to UAA", () => {
                JSDOM.fromFile("../index.html", { runScripts: "dangerously" }).then(dom => {
                    const login = dom.window.document.getElementById('login')
                    expect(login).toBeDefined()
                    const loginStatus = dom.window.document.getElementById('login-status')
                    expect(loginStatus).toBeDefined()
                })
            })
        })
    })
})
