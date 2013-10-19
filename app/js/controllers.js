'use strict';

// Stupid fucking linting warnings
var console = console;
var angular = angular;
var alert = alert;


var app = angular.module('InboxApp.controllers');
app.controller('AppContainerController',
    function($scope,
        $rootScope,
        wire,
        IBContact,
        $filter)
{

        $scope.contacts = []; // For UI element
        $scope.visible_contacts = [];


        $scope.performSearch = function(query) {
            if (query.length === 0) {
                $scope.clearSearch();
                return;
            }

            console.log(["Calling search", query]);

            wire.rpc('search', [query], function(rpc_data) {

                console.log(["Got response", rpc_data]);
                var fresh_contacts = [];

                angular.forEach(rpc_data, function(value, key) {
                    console.log(value);
                    var new_contact = new IBContact(value);

                    fresh_contacts.push(new_contact);
                });

                console.log(fresh_contacts);

            });
        };


        $scope.clearSearch = function() {
            console.log("We should clear the search filtering!");
            $scope.visible_contacts = $scope.contacts;
        };


        $scope.createContact = function(new_contact_info) {


        };


        $scope.performSearch('fish');
});
