# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

@StepClassMap @StepConcat
Feature: Step - concat()

  #TODO - Gherkins unfortunately doesn't allow checking for empty strings, either pass null through or provide work around in test definition
  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXa_bX_concat
    Given the modern graph
    And the traversal of
      """
      g.inject("a", "b").concat();
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | a |
      | b |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXa_bX_concat_XcX
    Given the modern graph
    And the traversal of
      """
      g.inject("a", "b").concat("c");
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | ac |
      | bc |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXa_b_cX_concat_d
    Given the modern graph
    And using the parameter xx1 defined as "l[a,b]"
    And the traversal of
      """
      g.inject(xx1,"c").concat("d");
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "String concat() can only take string as argument"

  Scenario: g_V_hasLabel_value_concat_X_X_concat_XpersonX
    Given the modern graph
    And the traversal of
      """
      g.V().hasLabel("person").values("name").concat(" ").concat("person")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | marko person |
      | vadas person |
      | josh person |
      | peter person |

    #TODO - add additional tests with modern graph

  Scenario: g_hasLabelXpersonX_valuesXnameX_asXaX_constantXMrX_concatXselect_aX
    Given the modern graph
    And the traversal of
      """
      g.V().hasLabel("person").values("name").as("a").constant("Mr.").concat(__.select("a"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | Mr.marko |
      | Mr.vadas |
      | Mr.josh |
      | Mr.peter |
