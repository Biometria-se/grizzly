Feature: grizzly example
  Background: common configuration
    Given "3" users
    And spawn rate is "1" user per second

  Scenario: dog facts api
    Given a user of type "RestApi" load testing "$conf::facts.dog.host"
    And repeat for "1" iteration
    And value for variable "AtomicRandomInteger.dog_facts_count" is "1..5"
    # custom step
    And also log successful requests
    Then get request with name "get-dog-facts" from endpoint "/api/v1/resources/dogs?number={{ AtomicRandomInteger.dog_facts_count }}"

  Scenario: cat facts api
    Given a user of type "RestApi" load testing "$conf::facts.cat.host"
    And repeat for "1" iteration
    And value for variable "AtomicRandomInteger.cat_facts_count" is "1..5"
    Then get request with name "get-cat-facts" from endpoint "/facts?limit={{ AtomicRandomInteger.cat_facts_count }}"

  Scenario: book api
    Given a user of type "RestApi" load testing "$conf::facts.book.host"
    And repeat for "1" iteration
    And value for variable "AtomicCsvRow.books" is "books/books.csv | random=True"
    And value for variable "author_endpoint" is "none"

    Then get request with name "1-get-book" from endpoint "/books/{{ AtomicCsvRow.books.book }}.json | content_type=json"
    When response payload "$.number_of_pages" is not "{{ AtomicCsvRow.books.pages }}" stop user
    When response payload "$.isbn_10[0]" is not "{{ AtomicCsvRow.books.isbn_10 }}" stop user
    Then save response payload "$.authors[0].key" in variable "author_endpoint"

    Then get request with name "2-get-author" from endpoint "{{ author_endpoint }}.json | content_type=json"
    When response payload "$.name" is not "{{ AtomicCsvRow.books.author }}" stop user

