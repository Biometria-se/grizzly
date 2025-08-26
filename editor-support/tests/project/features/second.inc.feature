Feature:
  Scenario: second
    Then log message "{$ bar $}=foo"
      | foo | bar |
      | bar | foo |

    # <!-- conditional steps -->
    {%- if {$ condition $} %}
    Then log message "{{ foobar }}"
    {%- endif %}
